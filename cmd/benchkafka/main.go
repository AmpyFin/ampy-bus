package main

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/AmpyFin/ampy-bus/pkg/ampybus"
	"github.com/AmpyFin/ampy-bus/pkg/kafkabinding"
)

type stats struct {
	mu        sync.Mutex
	latencies []time.Duration
	bytesPub  int64
	bytesRx   int64
	received  int64
}

func (s *stats) addLatency(d time.Duration) {
	s.mu.Lock(); s.latencies = append(s.latencies, d); s.mu.Unlock()
}
func (s *stats) addPub(n int)  { s.mu.Lock(); s.bytesPub += int64(n); s.mu.Unlock() }
func (s *stats) addRx(n int)   { s.mu.Lock(); s.bytesRx += int64(n); s.received++; s.mu.Unlock() }
func (s *stats) pct(p float64) time.Duration {
	s.mu.Lock(); defer s.mu.Unlock()
	if len(s.latencies) == 0 { return 0 }
	cp := append([]time.Duration(nil), s.latencies...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	k := int(p*float64(len(cp)-1) + 0.5)
	if k < 0 { k = 0 }; if k >= len(cp) { k = len(cp)-1 }
	return cp[k]
}

func main() {
	fs := flag.NewFlagSet("benchkafka", flag.ExitOnError)
	brokers := fs.String("brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	topic := fs.String("topic", "", "topic to bench (exact)")
	group := fs.String("group", "bench-consumer", "consumer group id")
	partitions := fs.Int("partitions", 3, "create topic with N partitions if missing")
	count := fs.Int("count", 1000, "messages to publish")
	concurrency := fs.Int("concurrency", 4, "parallel publishers")
	payloadBytes := fs.Int("payload-bytes", 256, "payload size (pre-compress)")
	compress := fs.Bool("gzip", false, "gzip payload")
	timeout := fs.Duration("timeout", 30*time.Second, "max bench duration")
	// SLO gates (0 = don’t enforce)
	sloP95ms := fs.Int("slo-p95-ms", 0, "fail if p95 latency exceeds (ms)")
	sloP99ms := fs.Int("slo-p99-ms", 0, "fail if p99 latency exceeds (ms)")
	minThroughput := fs.Float64("min-throughput", 0, "fail if msgs/sec below (0=disable)")
	fs.Parse(os.Args[1:])
	if *topic == "" { fs.PrintDefaults(); os.Exit(2) }

	cfg := kafkabinding.Config{
		Brokers: strings.Split(*brokers, ","),
		GroupID: *group,
	}
	bus := kafkabinding.New(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Ensure topic
	if err := bus.EnsureTopic(ctx, *topic, *partitions); err != nil {
		fmt.Fprintf(os.Stderr, "ensure topic: %v\n", err); os.Exit(1)
	}

	// RunID so the consumer filters only our run
	runID := fmt.Sprintf("bench-%d", time.Now().UnixNano())

	// Consumer
	st := &stats{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = bus.Subscribe(ctx, *topic, func(_ context.Context, env ampybus.Envelope) error {
			h := env.Headers
			if h.RunID != runID { return nil } // ignore other runs
			// compute latency now - produced_at
			if h.ProducedAt.IsZero() { return nil }
			lat := time.Since(h.ProducedAt)
			st.addLatency(lat)
			st.addRx(len(env.Payload))
			return nil
		})
	}()

	// Publishers
	start := time.Now()
	perPub := *count / *concurrency
	extra := map[string]string{}
	pubWg := sync.WaitGroup{}
	pubWg.Add(*concurrency)
	for w := 0; w < *concurrency; w++ {
		go func(idx int) {
			defer pubWg.Done()
			for i := 0; i < perPub; i++ {
				h := ampybus.NewHeaders("ampy.control.v1.Empty", fmt.Sprintf("bench@%d", idx), "bench", "pk")
				h.ContentType = "application/octet-stream"
				h.RunID = runID
				h.ProducedAt = time.Now().UTC()

				// payload
				raw := make([]byte, *payloadBytes)
				_, _ = rand.Read(raw)
				payload := raw
				if *compress {
					var buf strings.Builder
					gz := gzip.NewWriter(&nopWriteStringer{&buf})
					_, _ = gz.Write(raw)
					_ = gz.Close()
					payload = []byte(buf.String())
					h.ContentEncoding = "gzip"
				}

				env := ampybus.Envelope{Topic: *topic, Headers: h, Payload: payload}
				cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
				err := bus.PublishEnvelope(cctx, env, extra)
				ccancel()
				if err != nil { fmt.Fprintf(os.Stderr, "publish err: %v\n", err); return }
				st.addPub(len(payload))
			}
		}(w)
	}
	pubWg.Wait()

	// wait for receives or timeout
	want := perPub * *concurrency
	for {
		time.Sleep(100 * time.Millisecond)
		if int(st.received) >= want || time.Since(start) > *timeout {
			break
		}
	}

	elapsed := time.Since(start)
	// Summary
	p50 := st.pct(0.50); p95 := st.pct(0.95); p99 := st.pct(0.99)
	tput := float64(st.received) / elapsed.Seconds()
	fmt.Printf("[kafka-bench] topic=%s run_id=%s count=%d recv=%d elapsed=%s\n", *topic, runID, *count, st.received, elapsed)
	fmt.Printf("[kafka-bench] latency p50=%s p95=%s p99=%s\n", p50, p95, p99)
	fmt.Printf("[kafka-bench] throughput=%.1f msg/s pub_bytes=%d rx_bytes=%d\n", tput, st.bytesPub, st.bytesRx)

	// SLO gates
	fail := false
	if *sloP95ms > 0 && p95 > time.Duration(*sloP95ms)*time.Millisecond { fmt.Printf("[SLO] FAIL p95>%dms\n", *sloP95ms); fail = true }
	if *sloP99ms > 0 && p99 > time.Duration(*sloP99ms)*time.Millisecond { fmt.Printf("[SLO] FAIL p99>%dms\n", *sloP99ms); fail = true }
	if *minThroughput > 0 && tput < *minThroughput { fmt.Printf("[SLO] FAIL throughput<%.1f\n", *minThroughput); fail = true }
	if fail { os.Exit(1) }
}

// small adapter so we can gzip into a bytes buffer via string builder
type nopWriteStringer struct{ b *strings.Builder }
func (w *nopWriteStringer) Write(p []byte) (int, error) { return w.b.WriteString(string(p)) }

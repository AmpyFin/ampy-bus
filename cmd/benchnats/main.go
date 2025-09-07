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
	"github.com/AmpyFin/ampy-bus/pkg/ampybus/natsbinding"
)

type stats struct {
	mu        sync.Mutex
	latencies []time.Duration
	bytesPub  int64
	bytesRx   int64
	received  int64
}
func (s *stats) addLatency(d time.Duration){ s.mu.Lock(); s.latencies = append(s.latencies, d); s.mu.Unlock() }
func (s *stats) addPub(n int){ s.mu.Lock(); s.bytesPub += int64(n); s.mu.Unlock() }
func (s *stats) addRx(n int){ s.mu.Lock(); s.bytesRx += int64(n); s.received++; s.mu.Unlock() }
func (s *stats) pct(p float64) time.Duration {
	s.mu.Lock(); defer s.mu.Unlock()
	if len(s.latencies)==0 { return 0 }
	cp := append([]time.Duration(nil), s.latencies...)
	sort.Slice(cp, func(i,j int) bool { return cp[i] < cp[j] })
	k := int(p*float64(len(cp)-1) + 0.5)
	if k<0 {k=0}; if k>=len(cp){k=len(cp)-1}
	return cp[k]
}

func main() {
	fs := flag.NewFlagSet("benchnats", flag.ExitOnError)
	subject := fs.String("subject", "", "subject (exact)")
	natsURL := fs.String("nats", "nats://127.0.0.1:4222", "NATS URL")
	creds := fs.String("creds", "", ".creds file (optional)")
	nkeySeed := fs.String("nkey-seed", "", "NKey seed file (optional)")
	user := fs.String("user", "", "username"); pass := fs.String("pass", "", "password")
	tlsCA := fs.String("tls-ca", "", "TLS CA"); tlsCert := fs.String("tls-cert", "", "TLS cert"); tlsKey := fs.String("tls-key", "", "TLS key")
	insecure := fs.Bool("tls-insecure", false, "skip TLS verify (dev only)")
	count := fs.Int("count", 1000, "messages to publish")
	concurrency := fs.Int("concurrency", 4, "parallel publishers")
	payloadBytes := fs.Int("payload-bytes", 256, "payload size (pre-compress)")
	compress := fs.Bool("gzip", false, "gzip payload")
	timeout := fs.Duration("timeout", 30*time.Second, "max bench duration")
	sloP95ms := fs.Int("slo-p95-ms", 0, "fail if p95 latency exceeds (ms)")
	sloP99ms := fs.Int("slo-p99-ms", 0, "fail if p99 latency exceeds (ms)")
	minThroughput := fs.Float64("min-throughput", 0, "fail if msgs/sec below")
	fs.Parse(os.Args[1:])
	if *subject == "" { fs.PrintDefaults(); os.Exit(2) }

	cfg := natsbinding.Config{
		URLs:          *natsURL,
		StreamName:    "AMPY",
		Subjects:      []string{"ampy.>"},
		DurablePrefix: "bench",
		Auth: natsbinding.AuthConfig{
			UserCredsFile: *creds,
			NkeySeedFile:  *nkeySeed,
			Username:      *user,
			Password:      *pass,
		},
	}
	if *tlsCA != "" || (*tlsCert != "" && *tlsKey != "") || *insecure {
		cfg.TLS = &natsbinding.TLSConfig{
			CAFile:             *tlsCA,
			CertFile:           *tlsCert,
			KeyFile:            *tlsKey,
			InsecureSkipVerify: *insecure,
		}
	}

	bus, err := natsbinding.Connect(cfg)
	if err != nil { fmt.Fprintf(os.Stderr, "connect: %v\n", err); os.Exit(1) }
	defer bus.Close()
	if err := bus.EnsureStream(); err != nil { fmt.Fprintf(os.Stderr, "ensure stream: %v\n", err); os.Exit(1) }

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	runID := fmt.Sprintf("bench-%d", time.Now().UnixNano())

	// Consumer first
	st := &stats{}
	cancelSub, err := bus.SubscribePull(*subject, "bench-sub", func(_ context.Context, env ampybus.Envelope) error {
		if env.Headers.RunID != runID { return nil }
		if env.Headers.ProducedAt.IsZero() { return nil }
		lat := time.Since(env.Headers.ProducedAt)
		st.addLatency(lat)
		raw, _ := ampybus.DecodePayload(env.Payload, env.Headers.ContentEncoding)
		st.addRx(len(raw))
		return nil
	})
	if err != nil { fmt.Fprintf(os.Stderr, "subscribe: %v\n", err); os.Exit(1) }
	defer cancelSub()

	// Publishers
	start := time.Now()
	perPub := *count / *concurrency
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

				raw := make([]byte, *payloadBytes)
				_, _ = rand.Read(raw)
				payload := raw
				if *compress {
					var sb strings.Builder
					gz := gzip.NewWriter(&nopWriteStringer{&sb})
					_, _ = gz.Write(raw); _ = gz.Close()
					payload = []byte(sb.String())
					h.ContentEncoding = "gzip"
				}

				env := ampybus.Envelope{Topic: *subject, Headers: h, Payload: payload}
				cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := bus.PublishEnvelope(cctx, env, nil)
				ccancel()
				if err != nil { fmt.Fprintf(os.Stderr, "publish err: %v\n", err); return }
				st.addPub(len(payload))
			}
		}(w)
	}
	pubWg.Wait()

	// wait until received or timeout
	want := perPub * *concurrency
	for {
		time.Sleep(100 * time.Millisecond)
		if int(st.received) >= want || time.Since(start) > *timeout {
			break
		}
	}

	elapsed := time.Since(start)
	p50 := st.pct(0.50); p95 := st.pct(0.95); p99 := st.pct(0.99)
	tput := float64(st.received) / elapsed.Seconds()
	fmt.Printf("[nats-bench] subject=%s run_id=%s count=%d recv=%d elapsed=%s\n", *subject, runID, *count, st.received, elapsed)
	fmt.Printf("[nats-bench] latency p50=%s p95=%s p99=%s\n", p50, p95, p99)
	fmt.Printf("[nats-bench] throughput=%.1f msg/s pub_bytes=%d rx_bytes=%d\n", tput, st.bytesPub, st.bytesRx)

	fail := false
	if *sloP95ms > 0 && p95 > time.Duration(*sloP95ms)*time.Millisecond { fmt.Printf("[SLO] FAIL p95>%dms\n", *sloP95ms); fail = true }
	if *sloP99ms > 0 && p99 > time.Duration(*sloP99ms)*time.Millisecond { fmt.Printf("[SLO] FAIL p99>%dms\n", *sloP99ms); fail = true }
	if *minThroughput > 0 && tput < *minThroughput { fmt.Printf("[SLO] FAIL throughput<%.1f\n", *minThroughput); fail = true }
	if fail { os.Exit(1) }
}

type nopWriteStringer struct{ b *strings.Builder }
func (w *nopWriteStringer) Write(p []byte) (int, error) { return w.b.WriteString(string(p)) }

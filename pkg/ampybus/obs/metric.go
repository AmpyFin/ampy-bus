package obs

import (
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	once             sync.Once
	producedTotal    *prometheus.CounterVec
	consumedTotal    *prometheus.CounterVec
	deliveryLatency  *prometheus.HistogramVec
	batchSizeBytes   *prometheus.HistogramVec
	dlqTotal         *prometheus.CounterVec
	decodeFailTotal  *prometheus.CounterVec
)

func initRegistry() {
	producedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "bus", Name: "produced_total", Help: "Messages produced."},
		[]string{"topic", "producer"},
	)
	consumedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "bus", Name: "consumed_total", Help: "Messages consumed."},
		[]string{"topic", "consumer"},
	)
	deliveryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "bus", Name: "delivery_latency_ms", Help: "Latency (ms) from produced_at to consume.",
			Buckets: []float64{1, 2, 5, 10, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 2000, 5000},
		},
		[]string{"topic"},
	)
	batchSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "bus", Name: "batch_size_bytes", Help: "Payload size in bytes.",
			Buckets: prometheus.ExponentialBuckets(32, 2, 12), // 32..> 65k
		},
		[]string{"topic"},
	)
	dlqTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "bus", Name: "dlq_total", Help: "Messages routed to DLQ."},
		[]string{"topic", "reason"},
	)
	decodeFailTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Namespace: "bus", Name: "decode_fail_total", Help: "Payload decode/handler failures."},
		[]string{"topic", "reason"},
	)

	prometheus.MustRegister(producedTotal, consumedTotal, deliveryLatency, batchSizeBytes, dlqTotal, decodeFailTotal)
}

func EnsureRegistered() { once.Do(initRegistry) }

func StartMetricsServer(addr string) {
	EnsureRegistered()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() { _ = http.ListenAndServe(addr, mux) }()
}

func IncProduced(topic, producer string)                     { EnsureRegistered(); producedTotal.WithLabelValues(topic, producer).Inc() }
func IncConsumed(topic, consumer string)                     { EnsureRegistered(); consumedTotal.WithLabelValues(topic, consumer).Inc() }
func ObserveDeliveryLatency(topic string, producedAt, now time.Time) {
	EnsureRegistered()
	if !producedAt.IsZero() {
		ms := float64(now.Sub(producedAt).Milliseconds())
		if ms >= 0 {
			deliveryLatency.WithLabelValues(topic).Observe(ms)
		}
	}
}
func ObserveBatchSize(topic string, bytes int)               { EnsureRegistered(); if bytes >= 0 { batchSizeBytes.WithLabelValues(topic).Observe(float64(bytes)) } }
func IncDLQ(topic, reason string)                            { EnsureRegistered(); dlqTotal.WithLabelValues(topic, reason).Inc() }
func IncDecodeFail(topic, reason string)                     { EnsureRegistered(); decodeFailTotal.WithLabelValues(topic, reason).Inc() }

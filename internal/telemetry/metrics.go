package telemetry

import (
	"math"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds the catalog metrics. Every name here must have a row in docs/telemetry-catalog.md.
type Metrics struct {
	Registry *prometheus.Registry

	RequestsTotal   *prometheus.CounterVec   // shunt_requests_total{op,status_class,cluster,cluster_type}
	RequestDuration *prometheus.HistogramVec // shunt_request_duration_seconds{op}
	UpstreamTTFB    *prometheus.HistogramVec // shunt_upstream_ttfb_seconds{op,cluster}
	BytesIn         *prometheus.CounterVec   // shunt_bytes_in_total{op}
	BytesOut        *prometheus.CounterVec   // shunt_bytes_out_total{op}
	Inflight        *prometheus.GaugeVec     // shunt_inflight{op}
	AuthFailures    *prometheus.CounterVec   // shunt_auth_failures_total{reason}
	AuthDuration    *prometheus.HistogramVec // shunt_auth_duration_seconds{mode}
	Compensation    *prometheus.CounterVec   // shunt_compensation_total{reason,outcome}
}

// durationBuckets is 1 ms … 60 s, log-spaced, 16 buckets (docs/telemetry-catalog.md).
var durationBuckets = func() []float64 {
	const n = 16
	lo, hi := math.Log(0.001), math.Log(60)
	out := make([]float64, n)
	for i := range out {
		v := math.Exp(lo + (hi-lo)*float64(i)/float64(n-1))
		out[i] = math.Round(v*1e6) / 1e6 // 1 µs resolution; keeps the endpoints exact
	}
	return out
}()

// authBuckets is 10 µs … 100 ms, log-spaced, 16 buckets.
var authBuckets = func() []float64 {
	const n = 16
	lo, hi := math.Log(10e-6), math.Log(0.1)
	out := make([]float64, n)
	for i := range out {
		v := math.Exp(lo + (hi-lo)*float64(i)/float64(n-1))
		out[i] = math.Round(v*1e9) / 1e9
	}
	return out
}()

// NewMetrics registers the catalog metrics plus the Go and process collectors on a private registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shunt_requests_total", Help: "Requests completed, by S3 operation, response status class, and the cluster that served them (\"none\" when shunt answered itself).",
		}, []string{"op", "status_class", "cluster", "cluster_type"}),
		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "shunt_request_duration_seconds", Help: "Client-observed request duration, first byte in to last byte out.",
			Buckets: durationBuckets,
		}, []string{"op"}),
		UpstreamTTFB: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "shunt_upstream_ttfb_seconds", Help: "Upstream request sent to first response header byte.",
			Buckets: durationBuckets,
		}, []string{"op", "cluster"}),
		BytesIn: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shunt_bytes_in_total", Help: "Request body bytes received from clients.",
		}, []string{"op"}),
		BytesOut: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shunt_bytes_out_total", Help: "Response body bytes sent to clients.",
		}, []string{"op"}),
		Inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "shunt_inflight", Help: "Requests currently in flight.",
		}, []string{"op"}),
		AuthFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shunt_auth_failures_total", Help: "Client requests rejected by shunt's verifier in resign mode, by cause.",
		}, []string{"reason"}),
		AuthDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "shunt_auth_duration_seconds", Help: "Time to parse and verify the client signature.",
			Buckets: authBuckets,
		}, []string{"mode"}),
		Compensation: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "shunt_compensation_total", Help: "Late payload-check failures found after the upstream write (ADR-0002).",
		}, []string{"reason", "outcome"}),
	}
	reg.MustRegister(m.RequestsTotal, m.RequestDuration, m.UpstreamTTFB, m.BytesIn, m.BytesOut, m.Inflight,
		m.AuthFailures, m.AuthDuration, m.Compensation,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// StatusClass maps an HTTP status to its label value: "2xx", "3xx", "4xx", "5xx", or "0" when
// no response was produced (client disconnect before headers).
func StatusClass(status int) string {
	if status < 100 || status > 599 {
		return "0"
	}
	return strconv.Itoa(status/100) + "xx"
}

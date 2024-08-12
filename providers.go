package vshard_router //nolint:revive

import (
	"context"
	"log"
	"time"
)

var (
	_ MetricsProvider = (*EmptyMetrics)(nil)
	_ LogProvider     = (*EmptyLogger)(nil)
	_ LogProvider     = (*StdoutLogger)(nil)
)

type LogProvider interface {
	Info(context.Context, string)
	Debug(context.Context, string)
	Error(context.Context, string)
	Warn(context.Context, string)
}

type EmptyLogger struct{}

func (e *EmptyLogger) Info(_ context.Context, _ string)  {}
func (e *EmptyLogger) Debug(_ context.Context, _ string) {}
func (e *EmptyLogger) Error(_ context.Context, _ string) {}
func (e *EmptyLogger) Warn(_ context.Context, _ string)  {}

type StdoutLogger struct{}

func (e *StdoutLogger) Info(_ context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Debug(_ context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Error(_ context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Warn(_ context.Context, msg string) {
	log.Println(msg)
}

// Metrics

// MetricsProvider is an interface for passing library metrics to your prometheus/graphite and other metrics
type MetricsProvider interface {
	CronDiscoveryEvent(ok bool, duration time.Duration, reason string)
	RetryOnCall(reason string)
	RequestDuration(duration time.Duration, ok bool, mapReduce bool)
}

// EmptyMetrics is default empty metrics provider
// you can embed this type and realize just some metrics
type EmptyMetrics struct{}

func (e *EmptyMetrics) CronDiscoveryEvent(_ bool, _ time.Duration, _ string) {}
func (e *EmptyMetrics) RetryOnCall(_ string)                                 {}
func (e *EmptyMetrics) RequestDuration(_ time.Duration, _ bool, _ bool)      {}

// TopologyProvider is external module that can lookup current topology of cluster
// it might be etcd/config/consul or smth else
type TopologyProvider interface {
	// Init should create the current topology at the beginning
	// and change the state during the process of changing the point of receiving the cluster configuration
	Init(t TopologyController) error
	// Close closes all connections if the provider created them
	Close()
}

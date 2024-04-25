package vshard_router

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

func (e *EmptyLogger) Info(ctx context.Context, msg string)  {}
func (e *EmptyLogger) Debug(ctx context.Context, msg string) {}
func (e *EmptyLogger) Error(ctx context.Context, msg string) {}
func (e *EmptyLogger) Warn(ctx context.Context, msg string)  {}

type StdoutLogger struct{}

func (e *StdoutLogger) Info(ctx context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Debug(ctx context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Error(ctx context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Warn(ctx context.Context, msg string) {
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

func (e *EmptyMetrics) CronDiscoveryEvent(ok bool, duration time.Duration, reason string) {}
func (e *EmptyMetrics) RetryOnCall(reason string)                                         {}
func (e *EmptyMetrics) RequestDuration(duration time.Duration, ok bool, mapReduce bool)   {}

// TopologyProvider is external module that can lookup current topology of cluster
// it might be etcd/config/consul or smth else
type TopologyProvider interface {
	// Init should create the current topology at the beginning
	// and change the state during the process of changing the point of receiving the cluster configuration
	Init(t *TopologyController) error
	// Close closes all connections if the provider created them
	Close()
}

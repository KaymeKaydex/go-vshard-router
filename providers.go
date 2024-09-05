package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"log"
	"time"
)

var (
	emptyMetricsProvider MetricsProvider = (*EmptyMetrics)(nil)

	emptyLogfProvider LogfProvider = (*emptyLogger)(nil)
	_                 LogProvider  = (*StdoutLogger)(nil)
)

// A legacy interface for backward compatibility
type LogProvider interface {
	Info(context.Context, string)
	Debug(context.Context, string)
	Error(context.Context, string)
	Warn(context.Context, string)
}

type LogfProvider interface {
	Infof(ctx context.Context, format string, v ...any)
	Debugf(ctx context.Context, format string, v ...any)
	Errorf(ctx context.Context, format string, v ...any)
	Warnf(ctx context.Context, format string, v ...any)
}

// We use this type to support legacy logger api
type legacyLoggerProxy struct {
	l LogProvider
}

func (p *legacyLoggerProxy) Infof(ctx context.Context, format string, v ...any) {
	p.l.Info(ctx, fmt.Sprintf(format, v...))
}

func (p *legacyLoggerProxy) Debugf(ctx context.Context, format string, v ...any) {
	p.l.Debug(ctx, fmt.Sprintf(format, v...))
}

func (p *legacyLoggerProxy) Errorf(ctx context.Context, format string, v ...any) {
	p.l.Error(ctx, fmt.Sprintf(format, v...))
}

func (p *legacyLoggerProxy) Warnf(ctx context.Context, format string, v ...any) {
	p.l.Warn(ctx, fmt.Sprintf(format, v...))
}

type emptyLogger struct{}

func (e *emptyLogger) Infof(_ context.Context, _ string, _ ...any)  {}
func (e *emptyLogger) Debugf(_ context.Context, _ string, _ ...any) {}
func (e *emptyLogger) Errorf(_ context.Context, _ string, _ ...any) {}
func (e *emptyLogger) Warnf(_ context.Context, _ string, _ ...any)  {}

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

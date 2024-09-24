package vshard_router //nolint:revive

import (
	"context"
	"log"
	"time"
)

var (
	emptyMetricsProvider MetricsProvider = (*EmptyMetrics)(nil)
	emptyLogfProvider    LogfProvider    = emptyLogger{}

	// Ensure StdoutLoggerf implements LogfProvider
	_ LogfProvider = StdoutLoggerf{}
)

// LogfProvider an interface to inject a custom logger.
type LogfProvider interface {
	Debugf(ctx context.Context, format string, v ...any)
	Infof(ctx context.Context, format string, v ...any)
	Warnf(ctx context.Context, format string, v ...any)
	Errorf(ctx context.Context, format string, v ...any)
}

type emptyLogger struct{}

func (e emptyLogger) Debugf(_ context.Context, _ string, _ ...any) {}
func (e emptyLogger) Infof(_ context.Context, _ string, _ ...any)  {}
func (e emptyLogger) Warnf(_ context.Context, _ string, _ ...any)  {}
func (e emptyLogger) Errorf(_ context.Context, _ string, _ ...any) {}

// StdoutLogLevel is a type to control log level for StdoutLoggerf.
type StdoutLogLevel int

const (
	// StdoutLogDefault is equal to default value of StdoutLogLevel. Acts like StdoutLogInfo.
	StdoutLogDefault StdoutLogLevel = iota
	// StdoutLogDebug enables debug or higher level logs for StdoutLoggerf
	StdoutLogDebug
	// StdoutLogInfo enables only info or higher level logs for StdoutLoggerf
	StdoutLogInfo
	// StdoutLogWarn enables only warn or higher level logs for StdoutLoggerf
	StdoutLogWarn
	// StdoutLogError enables error level logs for StdoutLoggerf
	StdoutLogError
)

// StdoutLoggerf a logger that prints into stderr
type StdoutLoggerf struct {
	// LogLevel controls log level to print, see StdoutLogLevel constants for details.
	LogLevel StdoutLogLevel
}

func (s StdoutLoggerf) printLevel(level StdoutLogLevel, prefix string, format string, v ...any) {
	var currentLogLevel = s.LogLevel

	if currentLogLevel == StdoutLogDefault {
		currentLogLevel = StdoutLogInfo
	}

	if level >= currentLogLevel {
		log.Printf(prefix+format, v...)
	}
}

// Debugf implements Debugf method for LogfProvider interface
func (s StdoutLoggerf) Debugf(_ context.Context, format string, v ...any) {
	s.printLevel(StdoutLogDebug, "[DEBUG] ", format, v...)
}

// Infof implements Infof method for LogfProvider interface
func (s StdoutLoggerf) Infof(_ context.Context, format string, v ...any) {
	s.printLevel(StdoutLogInfo, "[INFO] ", format, v...)
}

// Warnf implements Warnf method for LogfProvider interface
func (s StdoutLoggerf) Warnf(_ context.Context, format string, v ...any) {
	s.printLevel(StdoutLogWarn, "[WARN] ", format, v...)
}

// Errorf implements Errorf method for LogfProvider interface
func (s StdoutLoggerf) Errorf(_ context.Context, format string, v ...any) {
	s.printLevel(StdoutLogError, "[ERROR] ", format, v...)
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

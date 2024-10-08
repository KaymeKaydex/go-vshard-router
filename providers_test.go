package vshard_router_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
)

var (
	emptyMetrics = vshardrouter.EmptyMetrics{}
	stdoutLogger = vshardrouter.StdoutLoggerf{}
)

func TestEmptyMetrics_RetryOnCall(t *testing.T) {
	require.NotPanics(t, func() {
		emptyMetrics.RetryOnCall("")
	})
}

func TestEmptyMetrics_RequestDuration(t *testing.T) {
	require.NotPanics(t, func() {
		emptyMetrics.RequestDuration(time.Second, false, false)
	})
}

func TestEmptyMetrics_CronDiscoveryEvent(t *testing.T) {
	require.NotPanics(t, func() {
		emptyMetrics.CronDiscoveryEvent(false, time.Second, "")
	})
}

func TestStdoutLogger(t *testing.T) {
	ctx := context.TODO()

	require.NotPanics(t, func() {
		stdoutLogger.Errorf(ctx, "")
	})
	require.NotPanics(t, func() {
		stdoutLogger.Infof(ctx, "")
	})
	require.NotPanics(t, func() {
		stdoutLogger.Warnf(ctx, "")
	})
	require.NotPanics(t, func() {
		stdoutLogger.Debugf(ctx, "")
	})
}

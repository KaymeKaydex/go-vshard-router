package tnt_test

import (
	"context"
	"log"
	"testing"
	"time"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"
	"github.com/stretchr/testify/require"
)

func TestRouterMapCall(t *testing.T) {
	if !isCorrectRun() {
		log.Printf("Incorrect run of tnt-test framework")
		return
	}

	t.Parallel()

	ctx := context.Background()

	cfg := getCfg()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(cfg),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             defaultTntUser,
		Password:         defaultTntPassword,
	})

	if err != nil {
		panic(err)
	}

	const arg = "arg1"

	// Enusre that RouterMapCallRWImpl works at all
	echoArgs := []interface{}{arg}
	resp, err := router.RouterMapCallRWImpl(ctx, "echo", echoArgs, vshardrouter.CallOpts{})
	require.NoError(t, err, "RouterMapCallRWImpl echo finished with no err")

	for k, v := range resp {
		require.Equalf(t, arg, v, "RouterMapCallRWImpl value ok for %v", k)
	}

	// Ensure that RouterMapCallRWImpl sends requests concurrently
	const sleepToSec int = 1
	sleepArgs := []interface{}{sleepToSec}

	start := time.Now()
	_, err = router.RouterMapCallRWImpl(ctx, "sleep", sleepArgs, vshardrouter.CallOpts{
		Timeout: 2 * time.Second, // because default timeout is 0.5 sec
	})
	duration := time.Since(start)

	require.NoError(t, err, "RouterMapCallRWImpl sleep finished with no err")
	require.Greater(t, len(cfg), 1, "There are more than one replicasets")
	require.Less(t, duration, 1200*time.Millisecond, "Requests were send concurrently")

	// Ensure that RouterMapCallRWImpl doesn't work when it mean't to
	for k := range cfg {
		errs := router.RemoveReplicaset(ctx, k.UUID)
		require.Emptyf(t, errs, "%s successfully removed from router", k.UUID)
		break
	}

	_, err = router.RouterMapCallRWImpl(ctx, "echo", echoArgs, vshardrouter.CallOpts{})
	require.NotNilf(t, err, "RouterMapCallRWImpl failed on not full cluster")
}

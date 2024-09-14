package tnt

import (
	"context"
	"log"
	"testing"
	"time"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

func TestReplicasetReplicaCall(t *testing.T) {
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

	require.Nil(t, err, "NewRouter finished successfully")

	rsMap := router.RouterRouteAll()

	var rs *vshardrouter.Replicaset
	// pick random rs
	for _, v := range rsMap {
		rs = v
		break
	}

	_ = rs.String() // just for coverage

	callOpts := vshardrouter.ReplicasetCallOpts{
		PoolMode: pool.ANY,
	}

	_, _, err = rs.ReplicaCall(ctx, callOpts, "echo", nil)
	require.NotNil(t, err, "ReplicaCall finished with err on nil args")

	_, _, err = rs.ReplicaCall(ctx, callOpts, "echo", []interface{}{})
	require.NotNil(t, err, "ReplicaCall returns err on empty response (broken interface)")

	// args len is 1
	args := []interface{}{"arg1"}
	resp, getTyped, err := rs.ReplicaCall(ctx, callOpts, "echo", args)
	require.Nilf(t, err, "ReplicaCall finished with no err for args: %v", args)
	require.Equalf(t, args[0], resp, "ReplicaCall resp ok for args: %v", args)
	var typed interface{}
	err = getTyped(&typed)
	require.Nilf(t, err, "getTyped finished with no err for args: %v", args)
	require.Equalf(t, args[0], typed, "getTyped result is ok for args: %v", args)

	// args len is 2
	args = []interface{}{"arg1", "arg2"}
	resp, getTyped, err = rs.ReplicaCall(ctx, callOpts, "echo", args)
	require.Nilf(t, err, "ReplicaCall finished with no err for args: %v", args)
	require.Equalf(t, args[0], resp, "ReplicaCall resp ok for args: %v", args)
	typed = nil // set to nil, otherwise getTyped tries to use the old content
	err = getTyped(&typed)
	require.Nilf(t, err, "getTyped finished with no err for args: %v", args)
	require.Equalf(t, args[0], typed, "getTyped result is ok for args: %v", args)

	// don't decode assert error
	args = []interface{}{nil, "non nil"}
	_, _, err = rs.ReplicaCall(ctx, callOpts, "echo", args)
	require.Nil(t, err, "ReplicaCall doesn't try decode assert error")

	args = []interface{}{2}
	callOpts.Timeout = 500 * time.Millisecond
	start := time.Now()
	_, _, err = rs.ReplicaCall(ctx, callOpts, "sleep", args)
	duration := time.Since(start)
	require.NotNil(t, err, "ReplicaCall timeout happened")
	require.Less(t, duration, 600*time.Millisecond, "ReplicaCall timeout works correctly")
	callOpts.Timeout = 0 // return back default value

	// raise_luajit_error
	_, _, err = rs.ReplicaCall(ctx, callOpts, "raise_luajit_error", nil)
	require.NotNil(t, err, "raise_luajit_error returns error")

	// raise_client_error
	_, _, err = rs.ReplicaCall(ctx, callOpts, "raise_client_error", nil)
	require.NotNil(t, err, "raise_client_error returns error")
}

func TestReplicsetCallAsync(t *testing.T) {
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

	require.Nil(t, err, "NewRouter finished successfully")

	rsMap := router.RouterRouteAll()

	var rs *vshardrouter.Replicaset
	// pick random rs
	for _, v := range rsMap {
		rs = v
		break
	}

	callOpts := vshardrouter.ReplicasetCallOpts{
		PoolMode: pool.ANY,
	}

	// Tests for arglen ans response parsing
	future := rs.CallAsync(ctx, callOpts, "echo", nil)
	resp, err := future.Get()
	require.Nil(t, err, "CallAsync finished with no err on nil args")
	require.Equal(t, resp, []interface{}{}, "CallAsync returns empty arr on nil args")
	var typed interface{}
	err = future.GetTyped(&typed)
	require.Nil(t, err, "GetTyped finished with no err on nil args")
	require.Equal(t, []interface{}{}, resp, "GetTyped returns empty arr on nil args")

	const checkUpTo = 100
	for argLen := 1; argLen <= checkUpTo; argLen++ {
		args := []interface{}{}

		for i := 0; i < argLen; i++ {
			args = append(args, "arg")
		}

		future := rs.CallAsync(ctx, callOpts, "echo", args)
		resp, err := future.Get()
		require.Nilf(t, err, "CallAsync finished with no err for argLen %d", argLen)
		require.Equalf(t, args, resp, "CallAsync resp ok for argLen %d", argLen)

		var typed interface{}
		err = future.GetTyped(&typed)
		require.Nilf(t, err, "GetTyped finished with no err for argLen %d", argLen)
		require.Equal(t, args, typed, "GetTyped resp ok for argLen %d", argLen)
	}

	// Test for async execution
	timeBefore := time.Now()

	var futures = make([]*tarantool.Future, 0, len(rsMap))
	for _, rs := range rsMap {
		future := rs.CallAsync(ctx, callOpts, "sleep", []interface{}{1})
		futures = append(futures, future)
	}

	for i, future := range futures {
		_, err := future.Get()
		require.Nil(t, err, "future[%d].Get finished with no err for async test", i)
	}

	duration := time.Since(timeBefore)
	require.True(t, len(rsMap) > 1, "Async test: more than one replicaset")
	require.Less(t, duration, 1200*time.Millisecond, "Async test: requests were sent concurrently")

	// Test no timeout by default
	future = rs.CallAsync(ctx, callOpts, "sleep", []interface{}{1})
	_, err = future.Get()
	require.Nil(t, err, "CallAsync no timeout by default")

	// Test for timeout via ctx
	ctxTimeout, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	future = rs.CallAsync(ctxTimeout, callOpts, "sleep", []interface{}{1})
	_, err = future.Get()
	require.NotNil(t, err, "CallAsync timeout by context does work")

	// Test for timeout via config
	callOptsTimeout := vshardrouter.ReplicasetCallOpts{
		PoolMode: pool.ANY,
		Timeout:  500 * time.Millisecond,
	}
	future = rs.CallAsync(ctx, callOptsTimeout, "sleep", []interface{}{1})
	_, err = future.Get()
	require.NotNil(t, err, "CallAsync timeout by callOpts does work")

	future = rs.CallAsync(ctx, callOpts, "raise_luajit_error", nil)
	_, err = future.Get()
	require.NotNil(t, err, "raise_luajit_error returns error")

	future = rs.CallAsync(ctx, callOpts, "raise_client_error", nil)
	_, err = future.Get()
	require.NotNil(t, err, "raise_client_error returns error")
}

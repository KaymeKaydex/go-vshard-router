package vshard_router //nolint:revive

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSearchLock_WaitOnSearch(t *testing.T) {
	lock := searchLock{
		mu:        sync.RWMutex{},
		perBucket: make([]chan struct{}, 10),
	}

	noLockStart := time.Now()
	lock.WaitOnSearch(5)
	require.True(t, time.Since(noLockStart) < time.Millisecond)

	lockStart := time.Now()
	chStopSearch := lock.StartSearch(3)

	go func() {
		time.Sleep(time.Millisecond * 10)
		close(chStopSearch)
	}()

	noLockStart = time.Now()
	lock.WaitOnSearch(5)
	require.True(t, time.Since(noLockStart) < time.Millisecond)

	lock.WaitOnSearch(3)

	require.True(t, time.Since(lockStart) < 12*time.Millisecond && time.Since(lockStart) > 9*time.Millisecond)
}

func TestRouter_DiscoveryAllBuckets(t *testing.T) {
	ctx := context.TODO()

	r := Router{
		cfg: Config{
			TotalBucketCount: uint64(256000),
			Logger:           &EmptyLogger{},
		},
	}

	err := r.DiscoveryAllBuckets(ctx)
	require.Error(t, err) // replicaset map is empty
}

func TestRouter_BucketResolve_InvalidBucketID(t *testing.T) {
	ctx := context.TODO()

	r := Router{
		cfg: Config{
			TotalBucketCount: uint64(10),
			Logger:           &EmptyLogger{},
		},
		routeMap: make([]*Replicaset, 11),
	}

	_, err := r.BucketResolve(ctx, 20)
	require.Error(t, err)
}

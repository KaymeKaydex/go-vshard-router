package tnt

import (
	"context"
	"testing"
	"time"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"
	"github.com/stretchr/testify/require"
)

func TestBucketDiscovery(t *testing.T) {
	skipOnInvalidRun(t)

	t.Parallel()

	var modes = []vshardrouter.BucketsSearchMode{
		vshardrouter.BucketsSearchLegacy,
		vshardrouter.BucketsSearchBatchedQuick,
		vshardrouter.BucketsSearchBatchedFull,
	}

	for _, mode := range modes {
		testBucketDiscoveryWithMode(t, mode)
	}
}

func testBucketDiscoveryWithMode(t *testing.T, searchMode vshardrouter.BucketsSearchMode) {
	ctx := context.Background()

	cfg := getCfg()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider:  static.NewProvider(cfg),
		DiscoveryTimeout:  5 * time.Second,
		DiscoveryMode:     vshardrouter.DiscoveryModeOnce,
		BucketsSearchMode: searchMode,
		TotalBucketCount:  totalBucketCount,
		User:              defaultTntUser,
		Password:          defaultTntPassword,
	})

	require.Nilf(t, err, "NewRouter finished successfully, mode %v", searchMode)

	// pick some random bucket
	bucketID := randBucketID(totalBucketCount)

	// clean everything
	router.RouteMapClean()

	// resolve it
	rs, err := router.BucketResolve(ctx, bucketID)
	require.Nilf(t, err, "BucketResolve ok, mode %v", searchMode)

	// reset it again
	router.BucketReset(bucketID)

	// call RouterRouteAll, because:
	// 1. increase coverage
	// 2. we cannot get replicaset uuid by rs instance (lack of interface)
	rsMap := router.RouterRouteAll()
	for k, v := range rsMap {
		if v == rs {
			// set it again
			res, err := router.BucketSet(bucketID, k)
			require.Nil(t, err, nil, "BucketSet ok")
			require.Equal(t, res, rs, "BucketSet res ok")
			break
		}
	}
}

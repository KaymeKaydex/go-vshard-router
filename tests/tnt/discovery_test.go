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

func TestBucketDiscovery(t *testing.T) {
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

	// pick some random bucket
	bucketID := randBucketID(totalBucketCount)

	// clean everything
	router.RouteMapClean()

	// resolve it
	rs, err := router.BucketResolve(ctx, bucketID)
	require.Nil(t, err, "BucketResolve ok")

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

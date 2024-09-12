package tnt_test

import (
	"context"
	"log"
	"math/rand"
	"testing"
	"time"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2/pool"
)

func TestRouterCall(t *testing.T) {
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

	//nolint:gosec
	bucketID := uint64((rand.Int() % totalBucketCount) + 1)
	args := []interface{}{"arg1"}

	resp, _, err := router.RouterCallImpl(ctx, bucketID, vshardrouter.CallOpts{
		VshardMode: vshardrouter.ReadMode,
		PoolMode:   pool.PreferRO,
	}, "echo", args)

	require.Nil(t, err, "RouterCallImpl echo finished with no err")
	require.EqualValues(t, args, resp, "RouterCallImpl echo resp correct")
}

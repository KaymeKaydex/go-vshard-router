package tnt

import (
	"context"
	"testing"
	"time"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2/pool"
)

func TestMsgpackDecodeErrors(t *testing.T) {
	skipOnInvalidRun(t)

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
	require.NoError(t, err)

	id := uuid.New()

	bucketID := router.RouterBucketIDStrCRC32(id.String())
	_, _, err = router.RouterCallImpl(
		ctx,
		bucketID,
		vshardrouter.CallOpts{VshardMode: vshardrouter.WriteMode, PoolMode: pool.RW, Timeout: 10 * time.Second},
		"non-existing",
		[]interface{}{&Product{Name: "test-go", BucketID: bucketID, ID: id.String(), Count: 3}})

	// TODO: errors is must work here
	require.Error(t, err)
}

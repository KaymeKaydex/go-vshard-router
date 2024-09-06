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
)

func TestTopology(t *testing.T) {
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

	var rsInfo vshardrouter.ReplicasetInfo
	var insInfo vshardrouter.InstanceInfo
	for k, replicas := range cfg {
		if len(replicas) == 0 {
			continue
		}
		rsInfo = k
		//nolint:gosec
		insInfo = replicas[rand.Int()%len(replicas)]
	}

	// remove some random replicaset
	_ = router.RemoveReplicaset(ctx, rsInfo.UUID)
	// add it again
	err = router.AddReplicasets(ctx, map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{rsInfo: cfg[rsInfo]})
	require.Nil(t, err, "AddReplicasets finished successfully")

	// remove some random instance
	err = router.RemoveInstance(ctx, rsInfo.UUID, insInfo.UUID)
	require.Nil(t, err, "RemoveInstance finished successfully")

	// add it again
	err = router.AddInstance(ctx, rsInfo.UUID, insInfo)
	require.Nil(t, err, "AddInstance finished successfully")
}

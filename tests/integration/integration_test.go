package integration

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	vshard_router "github.com/KaymeKaydex/go-vshard-router"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var server = "localhost:3013"

var dialer = tarantool.NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
}

func TestReplicasetCall(t *testing.T) {
	ctx := context.TODO()

	router, err := vshard_router.NewRouter(
		ctx,
		vshard_router.Config{
			User:             "test",
			Password:         "test",
			TotalBucketCount: 300,
			Replicasets: map[vshard_router.ReplicasetInfo][]vshard_router.InstanceInfo{
				{
					Name: "replicaset-1",
					UUID: uuid.New(),
				}: {
					{Addr: server, UUID: uuid.New()},
				},
			},
		})
	if err != nil {
		log.Fatalf("create router err: %s", err.Error())
	}

	var isReady bool

	for _, rs := range router.RouterRouteAll() {
		_, getTypedResp, err := rs.ReplicaCall(
			ctx,
			vshard_router.ReplicasetCallOpts{
				PoolMode: pool.RW,
			},
			"is_ready",
			[]interface{}{},
		)
		if err != nil {
			log.Fatalf("replicaset_call err: %s", err.Error())
		}

		err = getTypedResp(&isReady)
		if err != nil {
			log.Fatalf("getTypedResp err: %s", err.Error())
		}

		assert.Equal(t, true, isReady)
	}

	var (
		a   = int64(100)
		b   = int64(500)
		sum int64
	)

	for _, rs := range router.RouterRouteAll() {
		_, getTypedResp, err := rs.ReplicaCall(
			ctx,
			vshard_router.ReplicasetCallOpts{
				PoolMode: pool.RW,
			},
			"sum",
			[]interface{}{a, b},
		)
		if err != nil {
			log.Fatalf("replicaset_call err: %s", err.Error())
		}

		err = getTypedResp(&sum)
		if err != nil {
			log.Fatalf("getTypedResp err: %s", err.Error())
		}

		assert.Equal(t, a+b, sum)
	}

	for _, rs := range router.RouterRouteAll() {
		_, _, err := rs.ReplicaCall(
			ctx,
			vshard_router.ReplicasetCallOpts{
				PoolMode: pool.RW,
			},
			"up_error",
			[]interface{}{},
		)

		assert.Error(t, err)
	}
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       dialer,
		InitScript:   "testdata/config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(inst)

	if err != nil {
		log.Printf("Failed to prepare test tarantool: %s", err)
		return 1
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}

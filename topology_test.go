package vshard_router //nolint:revive

import (
	"context"
	"errors"
	"testing"

	mockpool "github.com/KaymeKaydex/go-vshard-router/mocks/pool"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRouter_Topology(t *testing.T) {
	router := Router{}

	require.NotNil(t, router.Topology())
}

func TestController_AddInstance(t *testing.T) {
	ctx := context.Background()

	t.Run("no such replicaset", func(t *testing.T) {
		router := Router{
			idToReplicaset: map[uuid.UUID]*Replicaset{},
		}

		err := router.Topology().AddInstance(ctx, uuid.New(), InstanceInfo{
			Addr: "127.0.0.1:8060",
			UUID: uuid.New(),
		})
		require.True(t, errors.Is(err, ErrReplicasetNotExists))
	})

	t.Run("invalid instance info", func(t *testing.T) {
		router := Router{
			idToReplicaset: map[uuid.UUID]*Replicaset{},
		}

		err := router.Topology().AddInstance(ctx, uuid.New(), InstanceInfo{})
		require.True(t, errors.Is(err, ErrInvalidInstanceInfo))
	})
}

func TestController_RemoveInstance(t *testing.T) {
	ctx := context.Background()

	t.Run("no such replicaset", func(t *testing.T) {
		router := Router{
			idToReplicaset: map[uuid.UUID]*Replicaset{},
		}

		err := router.Topology().RemoveInstance(ctx, uuid.New(), uuid.New())
		require.True(t, errors.Is(err, ErrReplicasetNotExists))
	})
}

func TestController_RemoveReplicaset(t *testing.T) {
	ctx := context.Background()

	uuidToRemove := uuid.New()
	mPool := mockpool.NewPool(t)
	mPool.On("CloseGraceful").Return(nil)

	router := Router{
		idToReplicaset: map[uuid.UUID]*Replicaset{
			uuidToRemove: {conn: mPool},
		},
	}

	t.Run("no such replicaset", func(t *testing.T) {
		t.Parallel()
		errs := router.Topology().RemoveReplicaset(ctx, uuid.New())
		require.True(t, errors.Is(errs[0], ErrReplicasetNotExists))
	})
	t.Run("successfully remove", func(t *testing.T) {
		t.Parallel()
		errs := router.Topology().RemoveReplicaset(ctx, uuidToRemove)
		require.Empty(t, errs)
	})
}

func TestRouter_AddReplicaset_AlreadyExists(t *testing.T) {
	ctx := context.TODO()

	alreadyExistingRsUUID := uuid.New()

	router := Router{
		idToReplicaset: map[uuid.UUID]*Replicaset{
			alreadyExistingRsUUID: {},
		},
	}

	// Test that such replicaset already exists
	err := router.AddReplicaset(ctx, ReplicasetInfo{UUID: alreadyExistingRsUUID}, []InstanceInfo{})
	require.Equalf(t, ErrReplicasetExists, err, "such replicaset must already exists")
}

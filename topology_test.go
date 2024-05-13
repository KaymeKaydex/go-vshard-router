package vshard_router

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

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

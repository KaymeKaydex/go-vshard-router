package vshard_router

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"

	mockpool "github.com/KaymeKaydex/go-vshard-router/mocks/pool"
)

func TestReplicasetInfo_String(t *testing.T) {
	rsUUID := uuid.New()
	rsInfo := ReplicasetInfo{
		Name: "test",
		UUID: rsUUID,
	}

	rs := Replicaset{
		info: rsInfo,
	}

	require.Equal(t, rsInfo.String(), rs.String())
	require.Contains(t, rsInfo.String(), "test")
	require.Contains(t, rsInfo.String(), rsUUID.String())
}

func TestReplicaset_BucketStat(t *testing.T) {
	ctx := context.TODO()
	rsUUID := uuid.New()
	rsInfo := ReplicasetInfo{
		Name: "test",
		UUID: rsUUID,
	}

	futureError := fmt.Errorf("testErr")

	errFuture := tarantool.NewFuture(tarantool.NewCallRequest("test"))
	errFuture.SetError(futureError)

	mPool := mockpool.NewPool(t)
	mPool.On("Do", mock.Anything, mock.Anything).Return(errFuture)

	rs := Replicaset{
		info: rsInfo,
		conn: mPool,
	}

	_, err := rs.BucketStat(ctx, 123)
	require.Equal(t, futureError, err)
}

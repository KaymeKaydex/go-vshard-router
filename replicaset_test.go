package vshard_router

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/vmihailenco/msgpack/v5"

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

	rs := Replicaset{
		info: rsInfo,
	}

	t.Run("pool do error", func(t *testing.T) {
		futureError := fmt.Errorf("testErr")

		errFuture := tarantool.NewFuture(tarantool.NewCallRequest("test"))
		errFuture.SetError(futureError)

		mPool := mockpool.NewPool(t)
		mPool.On("Do", mock.Anything, mock.Anything).Return(errFuture)
		rs.conn = mPool

		_, err := rs.BucketStat(ctx, 123)
		require.Equal(t, futureError, err)
	})

	t.Run("wrong bucket", func(t *testing.T) {
		f := tarantool.NewFuture(tarantool.NewCallRequest("vshard.storage.bucket_stat"))
		/*
			unix/:./data/storage_1_a.control> vshard.storage.bucket_stat(1000)
			---
			- null
			- bucket_id: 1000
			  reason: Not found
			  code: 1
			  type: ShardingError
			  message: 'Cannot perform action with bucket 1000, reason: Not found'
			  name: WRONG_BUCKET
			...
		*/
		bts, _ := msgpack.Marshal([]interface{}{1})

		err := f.SetResponse(tarantool.Header{}, bytes.NewReader(bts))
		require.NoError(t, err)

		mPool := mockpool.NewPool(t)
		mPool.On("Do", mock.Anything, mock.Anything).Return(f)
		rs.conn = mPool

		// todo: add real tests
		require.Panics(t, func() {
			_, err = rs.BucketStat(ctx, 123)
		})
	})
}

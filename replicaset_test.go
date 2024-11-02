package vshard_router //nolint:revive

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

	t.Run("unsupported or broken proto resp", func(t *testing.T) {
		f := tarantool.NewFuture(tarantool.NewCallRequest("vshard.storage.bucket_stat"))

		bts, _ := msgpack.Marshal([]interface{}{1})

		err := f.SetResponse(tarantool.Header{}, bytes.NewReader(bts))
		require.NoError(t, err)

		mPool := mockpool.NewPool(t)
		mPool.On("Do", mock.Anything, mock.Anything).Return(f)
		rs.conn = mPool

		// todo: add real tests

		statInfo, err := rs.BucketStat(ctx, 123)
		require.Error(t, err)
		require.Equal(t, statInfo, BucketStatInfo{BucketID: 0, Status: ""})
	})

	/*
		TODO: add test for wrong bucket response

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
}

func TestCalculateEtalonBalance(t *testing.T) {
	tests := []struct {
		name           string
		replicasets    []Replicaset
		bucketCount    uint64
		expectedCounts []uint64
		expectError    bool
	}{
		{
			name: "FullBalance",
			replicasets: []Replicaset{
				{info: ReplicasetInfo{Weight: 1, PinnedCount: 0, IgnoreDisbalance: false}},
				{info: ReplicasetInfo{Weight: 1, PinnedCount: 0, IgnoreDisbalance: false}},
				{info: ReplicasetInfo{Weight: 1, PinnedCount: 0, IgnoreDisbalance: false}},
			},
			bucketCount:    9,
			expectedCounts: []uint64{3, 3, 3},
			expectError:    false,
		},
		{
			name: "PinnedMoreThanWeight",
			replicasets: []Replicaset{
				{info: ReplicasetInfo{Weight: 1, PinnedCount: 60, IgnoreDisbalance: false}},
				{info: ReplicasetInfo{Weight: 1, PinnedCount: 0, IgnoreDisbalance: false}},
			},
			bucketCount:    100,
			expectedCounts: []uint64{60, 40},
			expectError:    false,
		},
		{
			name: "ZeroWeight",
			replicasets: []Replicaset{
				{info: ReplicasetInfo{Weight: 0, PinnedCount: 0, IgnoreDisbalance: false}},
				{info: ReplicasetInfo{Weight: 1, PinnedCount: 0, IgnoreDisbalance: false}},
			},
			bucketCount:    10,
			expectError:    false,
			expectedCounts: []uint64{0, 10},
		},
		{
			name: "ZeroAllWeights",
			replicasets: []Replicaset{
				{info: ReplicasetInfo{Weight: 0, PinnedCount: 0, IgnoreDisbalance: false}},
				{info: ReplicasetInfo{Weight: 0, PinnedCount: 0, IgnoreDisbalance: false}},
			},
			bucketCount: 10,
			expectError: true,
		},
		{
			name: "UnevenDistribution",
			replicasets: []Replicaset{
				{info: ReplicasetInfo{Weight: 1, PinnedCount: 0, IgnoreDisbalance: false}},
				{info: ReplicasetInfo{Weight: 2, PinnedCount: 0, IgnoreDisbalance: false}},
			},
			bucketCount:    7,
			expectError:    false,
			expectedCounts: []uint64{2, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CalculateEtalonBalance(tt.replicasets, tt.bucketCount)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				for i, expectedCount := range tt.expectedCounts {
					require.Equal(t, expectedCount, tt.replicasets[i].EtalonBucketCount)
				}
			}
		})
	}
}

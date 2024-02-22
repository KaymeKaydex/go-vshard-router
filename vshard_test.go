package vshard_router

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2/pool"
)

func TestNewRouter_EmptyReplicasets(t *testing.T) {
	ctx := context.TODO()

	router, err := NewRouter(ctx, Config{})
	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouter_InvalidReplicasetUUID(t *testing.T) {
	ctx := context.TODO()

	router, err := NewRouter(ctx, Config{
		Replicasets: map[ReplicasetInfo][]InstanceInfo{
			ReplicasetInfo{
				Name: "123",
			}: []InstanceInfo{
				{Addr: "first.internal:1212"},
			},
		},
	})

	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouter_InstanceAddr(t *testing.T) {
	ctx := context.TODO()

	router, err := NewRouter(ctx, Config{
		Replicasets: map[ReplicasetInfo][]InstanceInfo{
			ReplicasetInfo{
				Name: "123",
				UUID: uuid.New(),
			}: []InstanceInfo{
				{Addr: "first.internal:1212"},
			},
		},
	})

	require.Error(t, err)
	require.Nil(t, router)
}

func TestRouterBucketIDStrCRC32(t *testing.T) {
	// required values from tarantool example
	require.Equal(t, uint64(103202), BucketIDStrCRC32("2707623829", uint64(256000)))
	require.Equal(t, uint64(35415), BucketIDStrCRC32("2706201716", uint64(256000)))
}

func TestNewRouter_Real(t *testing.T) {
	return
	ctx := context.TODO()

	rs1UUID, err := uuid.Parse("6E35AC64-1241-0001-1000-000000000000")
	require.NoError(t, err)
	require.NotNil(t, rs1UUID)

	rs2UUID, _ := uuid.Parse("6E35AC64-1241-0002-1000-000000000000")

	profile11, _ := uuid.Parse("6E35AC64-1241-0001-0001-000000000000")
	profile12, _ := uuid.Parse("6E35AC64-1241-0001-0002-000000000000")

	profile21, _ := uuid.Parse("6E35AC64-1241-0002-0001-000000000000")
	profile22, _ := uuid.Parse("6E35AC64-1241-0002-0002-000000000000")

	router, err := NewRouter(ctx, Config{
		TotalBucketCount: 256000,
		User:             "guest",
		Replicasets: map[ReplicasetInfo][]InstanceInfo{
			ReplicasetInfo{
				Name: "profile_001",
				UUID: rs1UUID,
			}: []InstanceInfo{
				{Addr: "cld-profile.minidev:25836", UUID: profile11},
				{Addr: "cld-profile.minidev:25837", UUID: profile12},
			},
			ReplicasetInfo{
				Name: "profile_002",
				UUID: rs2UUID,
			}: []InstanceInfo{
				{Addr: "cld-profile.minidev:25838", UUID: profile21},
				{Addr: "cld-profile.minidev:25839", UUID: profile22},
			},
		},
		Logger: &StdoutLogger{},
	})

	require.Nil(t, err)
	require.NotNil(t, router)

	bucketID := BucketIDStrCRC32("2707623829", 256000)

	rs, err := router.BucketResolve(ctx, bucketID)
	fmt.Println(rs)

	for {
		res, _, err := router.RouterCallImpl(ctx, bucketID, CallOpts{VshardMode: WriteMode, PoolMode: pool.RW}, "profile.api.get_email_state", []interface{}{
			&struct {
				ID       int                    `msgpack:"id" json:"id,omitempty"`
				BucketID uint64                 `msgpack:"bucket_id" json:"bucket_id,omitempty"`
				S2S      bool                   `msgpack:"s2s,omitempty"`
				Body     map[string]interface{} `msgpack:"body"`
			}{
				ID:       123,
				BucketID: bucketID,
				S2S:      true,
				Body: map[string]interface{}{
					"uid": 2707623829,
				},
			},
		})

		if err != nil {
			fmt.Println(err)
			panic(err)
		}

		fmt.Println(reflect.TypeOf(res))

		if res == nil && err == nil {
			panic("wtf")
		}

		resSlices := res.([]interface{})
		fmt.Println(resSlices[0])

		if resSlices[0].(uint8) == 200 {
			continue
		}

		fmt.Println(resSlices[0])
	}

}

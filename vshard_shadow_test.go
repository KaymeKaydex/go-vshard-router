package vshard_router_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	vshard_router "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"
)

func TestNewRouter_EmptyReplicasets(t *testing.T) {
	ctx := context.TODO()

	router, err := vshard_router.NewRouter(ctx, vshard_router.Config{})
	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouter_InvalidReplicasetUUID(t *testing.T) {
	ctx := context.TODO()

	router, err := vshard_router.NewRouter(ctx, vshard_router.Config{
		TopologyProvider: static.NewProvider(map[vshard_router.ReplicasetInfo][]vshard_router.InstanceInfo{
			vshard_router.ReplicasetInfo{
				Name: "123",
			}: {
				{Addr: "first.internal:1212"},
			},
		}),
	})

	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouter_InstanceAddr(t *testing.T) {
	ctx := context.TODO()

	router, err := vshard_router.NewRouter(ctx, vshard_router.Config{
		TopologyProvider: static.NewProvider(map[vshard_router.ReplicasetInfo][]vshard_router.InstanceInfo{
			vshard_router.ReplicasetInfo{
				Name: "123",
				UUID: uuid.New(),
			}: {
				{Addr: "first.internal:1212"},
			},
		}),
	})

	require.Error(t, err)
	require.Nil(t, router)
}

func TestRouterBucketIDStrCRC32(t *testing.T) {
	// required values from tarantool example
	require.Equal(t, uint64(103202), vshard_router.BucketIDStrCRC32("2707623829", uint64(256000)))
	require.Equal(t, uint64(35415), vshard_router.BucketIDStrCRC32("2706201716", uint64(256000)))
}

func BenchmarkRouterBucketIDStrCRC32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vshard_router.BucketIDStrCRC32("test_bench_key", uint64(256000))
	}
}

func TestInstanceInfo_Validate(t *testing.T) {
	tCases := []struct {
		Name  string
		II    vshard_router.InstanceInfo
		Valid bool
	}{
		{
			Name:  "no info",
			II:    vshard_router.InstanceInfo{},
			Valid: false,
		},
		{
			Name:  "no uuid",
			II:    vshard_router.InstanceInfo{Addr: "first.internal:1212"},
			Valid: false,
		},
		{
			Name:  "no addr",
			II:    vshard_router.InstanceInfo{UUID: uuid.New()},
			Valid: false,
		},
		{
			Name:  "ok",
			II:    vshard_router.InstanceInfo{UUID: uuid.New(), Addr: "first.internal:1212"},
			Valid: true,
		},
	}

	for _, tc := range tCases {
		t.Run(tc.Name, func(t *testing.T) {
			err := tc.II.Validate()
			if tc.Valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

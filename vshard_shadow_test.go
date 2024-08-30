package vshard_router_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"
)

type errorTopologyProvider struct{}

func (e *errorTopologyProvider) Init(_ vshardrouter.TopologyController) error {
	return fmt.Errorf("test error")
}
func (e *errorTopologyProvider) Close() {}

func TestNewRouter_ProviderError(t *testing.T) {
	ctx := context.TODO()
	_, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TotalBucketCount: 256000,
		TopologyProvider: &errorTopologyProvider{},
	})

	require.ErrorIs(t, err, vshardrouter.ErrTopologyProvider)
}

func TestNewRouter_EmptyReplicasets(t *testing.T) {
	ctx := context.TODO()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{})
	require.Error(t, err)
	require.Nil(t, router)
}

func TestNewRouter_InvalidReplicasetUUID(t *testing.T) {
	ctx := context.TODO()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
			{
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

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{
			{
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
	require.Equal(t, uint64(103202), vshardrouter.BucketIDStrCRC32("2707623829", uint64(256000)))
	require.Equal(t, uint64(35415), vshardrouter.BucketIDStrCRC32("2706201716", uint64(256000)))
}

func BenchmarkRouterBucketIDStrCRC32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		vshardrouter.BucketIDStrCRC32("test_bench_key", uint64(256000))
	}
}

func TestInstanceInfo_Validate(t *testing.T) {
	tCases := []struct {
		Name  string
		II    vshardrouter.InstanceInfo
		Valid bool
	}{
		{
			Name:  "no info",
			II:    vshardrouter.InstanceInfo{},
			Valid: false,
		},
		{
			Name:  "no uuid",
			II:    vshardrouter.InstanceInfo{Addr: "first.internal:1212"},
			Valid: false,
		},
		{
			Name:  "no addr",
			II:    vshardrouter.InstanceInfo{UUID: uuid.New()},
			Valid: false,
		},
		{
			Name:  "ok",
			II:    vshardrouter.InstanceInfo{UUID: uuid.New(), Addr: "first.internal:1212"},
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

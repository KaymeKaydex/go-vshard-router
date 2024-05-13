package vshard_router

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouter_RouterBucketIDStrCRC32(t *testing.T) {
	r := Router{
		cfg: Config{TotalBucketCount: uint64(256000)},
	}

	t.Run("deprecated old logic", func(t *testing.T) {
		require.Equal(t, uint64(103202), r.RouterBucketID("2707623829"))
	})
	t.Run("new logic with current hash sum", func(t *testing.T) {
		require.Equal(t, uint64(103202), r.RouterBucketIDStrCRC32("2707623829"))

	})
}

func TestRouter_RouterBucketCount(t *testing.T) {
	bucketCount := uint64(123)

	r := Router{
		cfg: Config{TotalBucketCount: bucketCount},
	}

	require.Equal(t, bucketCount, r.RouterBucketCount())
}

func TestRouter_RouteMapClean(t *testing.T) {
	r := Router{
		cfg:      Config{TotalBucketCount: 10},
		routeMap: make([]*Replicaset, 10),
	}

	require.NotPanics(t, func() {
		r.RouteMapClean()
	})

}

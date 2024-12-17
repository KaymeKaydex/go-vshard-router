package vshard_router //nolint:revive

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouter_BucketResolve_InvalidBucketID(t *testing.T) {
	ctx := context.TODO()

	r := Router{
		cfg: Config{
			TotalBucketCount: uint64(10),
			Loggerf:          emptyLogfProvider,
		},
		concurrentData: newRouterConcurrentData(10),
	}

	_, err := r.BucketResolve(ctx, 20)
	require.Error(t, err)
}

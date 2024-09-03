package vshard_router //nolint:revive

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouter_BucketResolve_InvalidBucketID(t *testing.T) {
	ctx := context.TODO()

	r := Router{
		cfg: Config{
			TotalBucketCount: uint64(10),
			Logger:           &EmptyLogger{},
		},
		view: &consistentView{
			routeMap: make([]atomic.Pointer[Replicaset], 11),
		},
	}

	_, err := r.BucketResolve(ctx, 20)
	require.Error(t, err)
}

package vshard_router // nolint: revive

import (
	"context"
	"fmt"
	"testing"
	"time"

	mockpool "github.com/KaymeKaydex/go-vshard-router/mocks/pool"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
)

var emptyRouter = &Router{
	cfg: Config{
		TotalBucketCount: uint64(10),
		Loggerf:          emptyLogfProvider,
		Metrics:          emptyMetricsProvider,
	},
	concurrentData: newRouterConcurrentData(10),
}

func TestVshardMode_String_NotEmpty(t *testing.T) {
	t.Parallel()
	require.NotEmpty(t, ReadMode.String())
	require.NotEmpty(t, WriteMode.String())
}

func TestRouter_RouterRouteAll(t *testing.T) {
	t.Parallel()
	m := emptyRouter.RouterRouteAll()
	require.Empty(t, m)
}

func TestRouter_RouterCallImpl(t *testing.T) {
	t.Parallel()
	ctx := context.TODO()

	t.Run("bucket id is out of range", func(t *testing.T) {
		t.Parallel()

		_, _, err := emptyRouter.RouterCallImpl(ctx, 100, CallOpts{}, "test", []byte("test"))
		require.Errorf(t, err, "bucket id is out of range")
	})
	t.Run("future error when router call impl", func(t *testing.T) {
		t.Parallel()
		r := &Router{
			cfg: Config{
				TotalBucketCount: uint64(10),
				Loggerf:          emptyLogfProvider,
				Metrics:          emptyMetricsProvider,
			},
			concurrentData: newRouterConcurrentData(10),
		}

		futureError := fmt.Errorf("testErr")
		errFuture := tarantool.NewFuture(tarantool.NewCallRequest("test"))
		errFuture.SetError(futureError)

		mPool := mockpool.NewPool(t)
		mPool.On("Do", mock.Anything, mock.Anything).Return(errFuture)

		r.concurrentData.(*routerConcurrentDataImpl).view.routeMap[5].Store(&Replicaset{
			conn: mPool,
		})

		_, _, err := r.RouterCallImpl(ctx, 5, CallOpts{Timeout: time.Second}, "test", []byte("test"))
		require.ErrorIs(t, err, futureError)
	})
}

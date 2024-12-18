package tnt

import (
	"context"
	"testing"
	"time"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/KaymeKaydex/go-vshard-router/providers/static"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

type Product struct {
	BucketID uint64 `msgpack:"bucket_id"`
	ID       string `msgpack:"id"`
	Name     string `msgpack:"name"`
	Count    uint64 `msgpack:"count"`
}

func BenchmarkCallSimpleInsert_GO_RouterCall(b *testing.B) {
	b.StopTimer()
	skipOnInvalidRun(b)

	ctx := context.Background()

	cfg := getCfg()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(cfg),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             defaultTntUser,
		Password:         defaultTntPassword,
		RequestTimeout:   time.Minute,
	})
	require.NoError(b, err)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := uuid.New()

		bucketID := router.RouterBucketIDStrCRC32(id.String())
		_, _, err := router.RouterCallImpl(
			ctx,
			bucketID,
			vshardrouter.CallOpts{VshardMode: vshardrouter.WriteMode, PoolMode: pool.RW, Timeout: 10 * time.Second},
			"product_add",
			[]interface{}{&Product{Name: "test-go", BucketID: bucketID, ID: id.String(), Count: 3}})
		require.NoError(b, err)
	}

	b.ReportAllocs()
}

func BenchmarkCallSimpleInsert_GO_Call(b *testing.B) {
	b.StopTimer()
	skipOnInvalidRun(b)

	ctx := context.Background()

	cfg := getCfg()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(cfg),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             defaultTntUser,
		Password:         defaultTntPassword,
		RequestTimeout:   time.Minute,
	})
	require.NoError(b, err)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := uuid.New()

		bucketID := router.RouterBucketIDStrCRC32(id.String())
		_, err := router.Call(
			ctx,
			bucketID,
			vshardrouter.VshardRouterCallModeRW,
			"product_add",
			[]interface{}{&Product{Name: "test-go", BucketID: bucketID, ID: id.String(), Count: 3}},
			vshardrouter.VshardRouterCallOptions{Timeout: 10 * time.Second})
		require.NoError(b, err)
	}

	b.ReportAllocs()
}

func BenchmarkCallSimpleInsert_Lua(b *testing.B) {
	b.StopTimer()

	skipOnInvalidRun(b)

	ctx := context.Background()
	dialer := tarantool.NetDialer{
		Address: "0.0.0.0:12000",
	}

	instances := []pool.Instance{{
		Name:   "router",
		Dialer: dialer,
	}}

	p, err := pool.Connect(ctx, instances)
	require.NoError(b, err)
	require.NotNil(b, p)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := uuid.New()
		req := tarantool.NewCallRequest("api.add_product").
			Context(ctx).
			Args([]interface{}{&Product{Name: "test-lua", ID: id.String(), Count: 3}})

		feature := p.Do(req, pool.ANY)
		faces, err := feature.Get()

		require.NoError(b, err)
		require.NotNil(b, faces)
	}

	b.ReportAllocs()
}

func BenchmarkCallSimpleSelect_GO_RouterCall(b *testing.B) {
	b.StopTimer()
	skipOnInvalidRun(b)

	ctx := context.Background()

	cfg := getCfg()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(cfg),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             defaultTntUser,
		Password:         defaultTntPassword,
	})
	require.NoError(b, err)

	ids := make([]uuid.UUID, b.N)

	for i := 0; i < b.N; i++ {
		id := uuid.New()
		ids[i] = id

		bucketID := router.RouterBucketIDStrCRC32(id.String())
		_, _, err := router.RouterCallImpl(
			ctx,
			bucketID,
			vshardrouter.CallOpts{VshardMode: vshardrouter.WriteMode, PoolMode: pool.RW},
			"product_add",
			[]interface{}{&Product{Name: "test-go", BucketID: bucketID, ID: id.String(), Count: 3}})
		require.NoError(b, err)
	}

	type Request struct {
		ID string `msgpack:"id"`
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i]

		bucketID := router.RouterBucketIDStrCRC32(id.String())
		_, getTyped, err1 := router.RouterCallImpl(
			ctx,
			bucketID,
			vshardrouter.CallOpts{VshardMode: vshardrouter.ReadMode, PoolMode: pool.ANY, Timeout: time.Second},
			"product_get",
			[]interface{}{&Request{ID: id.String()}})

		var product Product
		err2 := getTyped(&product)

		b.StopTimer()
		require.NoError(b, err1)
		require.NoError(b, err2)
		b.StartTimer()
	}

	b.ReportAllocs()
}

func BenchmarkCallSimpleSelect_GO_Call(b *testing.B) {
	b.StopTimer()
	skipOnInvalidRun(b)

	ctx := context.Background()

	cfg := getCfg()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(cfg),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             defaultTntUser,
		Password:         defaultTntPassword,
	})
	require.NoError(b, err)

	ids := make([]uuid.UUID, b.N)

	for i := 0; i < b.N; i++ {
		id := uuid.New()
		ids[i] = id

		bucketID := router.RouterBucketIDStrCRC32(id.String())
		_, err := router.Call(
			ctx,
			bucketID,
			vshardrouter.VshardRouterCallModeRW,
			"product_add",
			[]interface{}{&Product{Name: "test-go", BucketID: bucketID, ID: id.String(), Count: 3}},
			vshardrouter.VshardRouterCallOptions{},
		)
		require.NoError(b, err)
	}

	type Request struct {
		ID string `msgpack:"id"`
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i]

		bucketID := router.RouterBucketIDStrCRC32(id.String())
		resp, err1 := router.Call(
			ctx,
			bucketID,
			vshardrouter.VshardRouterCallModeBRO,
			"product_get",
			[]interface{}{&Request{ID: id.String()}},
			vshardrouter.VshardRouterCallOptions{Timeout: time.Second},
		)

		var product Product
		err2 := resp.GetTyped([]interface{}{&product})

		b.StopTimer()
		require.NoError(b, err1)
		require.NoError(b, err2)
		b.StartTimer()
	}

	b.ReportAllocs()
}

func BenchmarkCallSimpleSelect_Lua(b *testing.B) {
	b.StopTimer()
	skipOnInvalidRun(b)

	ctx := context.Background()

	cfg := getCfg()

	router, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		TopologyProvider: static.NewProvider(cfg),
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             defaultTntUser,
		Password:         defaultTntPassword,
	})
	require.NoError(b, err)

	ids := make([]uuid.UUID, b.N)

	for i := 0; i < b.N; i++ {
		id := uuid.New()
		ids[i] = id

		bucketID := router.RouterBucketIDStrCRC32(id.String())
		_, _, err := router.RouterCallImpl(
			ctx,
			bucketID,
			vshardrouter.CallOpts{VshardMode: vshardrouter.WriteMode, PoolMode: pool.RW},
			"product_add",
			[]interface{}{&Product{Name: "test-go", BucketID: bucketID, ID: id.String(), Count: 3}})
		require.NoError(b, err)
	}

	type Request struct {
		ID string `msgpack:"id"`
	}

	dialer := tarantool.NetDialer{
		Address: "0.0.0.0:12000",
	}

	instances := []pool.Instance{{
		Name:   "router",
		Dialer: dialer,
	}}

	p, err := pool.Connect(ctx, instances)
	require.NoError(b, err)
	require.NotNil(b, p)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i]

		req := tarantool.NewCallRequest("api.get_product").
			Context(ctx).
			Args([]interface{}{&Request{ID: id.String()}})

		feature := p.Do(req, pool.ANY)
		var product Product
		err := feature.GetTyped(&[]interface{}{&product})

		b.StopTimer()
		require.NoError(b, err)
		b.StartTimer()
	}

	b.ReportAllocs()
}

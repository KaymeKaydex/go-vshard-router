package vshard_router

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/snksoft/crc"

	tarantool "github.com/tarantool/go-tarantool/v2"
)

var ErrInvalidConfig = fmt.Errorf("config invalid")

type Router struct {
	cfg Config

	idToReplicaset map[uuid.UUID]*Replicaset
	routeMap       []*Replicaset
	searchLock     searchLock

	knownBucketCount atomic.Int32

	// ----------------------- Map-Reduce -----------------------
	// Storage Ref ID. It must be unique for each ref request
	// and therefore is global and monotonically growing.
	refID atomic.Int64

	// worker's count to proceed channel of replicaset's futures
	nWorkers int32

	cancelDiscovery func()
}

func (r *Router) metrics() MetricsProvider {
	return r.cfg.Metrics
}
func (r *Router) log() LogProvider {
	return r.cfg.Logger
}

type Config struct {
	// Providers
	Logger           LogProvider      // Logger is not required
	Metrics          MetricsProvider  // Metrics is not required
	TopologyProvider TopologyProvider // TopologyProvider is required provider

	// Discovery
	DiscoveryTimeout time.Duration // DiscoveryTimeout is timeout between cron discovery job; by default there is no timeout
	DiscoveryMode    DiscoveryMode

	TotalBucketCount uint64
	User             string
	Password         string
	PoolOpts         tarantool.Opts

	NWorkers int32 // todo: rename this, cause NWorkers naming looks strange
}

type BucketStatInfo struct {
	BucketID uint64 `mapstructure:"id"`
	Status   string `mapstructure:"status"`
}

type InstanceInfo struct {
	Addr string
	UUID uuid.UUID
}

// --------------------------------------------------------------------------------
// -- Configuration
// --------------------------------------------------------------------------------

func NewRouter(ctx context.Context, cfg Config) (*Router, error) {
	var err error

	cfg, err = prepareCfg(ctx, cfg)
	if err != nil {
		return nil, err
	}

	router := &Router{
		cfg:              cfg,
		idToReplicaset:   make(map[uuid.UUID]*Replicaset),
		routeMap:         make([]*Replicaset, cfg.TotalBucketCount+1),
		searchLock:       searchLock{mu: sync.RWMutex{}, perBucket: make([]chan struct{}, cfg.TotalBucketCount+1)},
		knownBucketCount: atomic.Int32{},
	}

	router.knownBucketCount.Store(0)

	err = cfg.TopologyProvider.Init(router.Topology())
	if err != nil {
		router.log().Error(ctx, fmt.Sprintf("cant create new topology provider with err: %s", err))

		return nil, err
	}

	err = router.DiscoveryAllBuckets(ctx)
	if err != nil {
		return nil, err
	}

	if cfg.DiscoveryMode == DiscoveryModeOn {
		discoveryCronCtx, cancelFunc := context.WithCancel(context.Background())

		go func() {
			discoveryErr := router.startCronDiscovery(discoveryCronCtx)
			if discoveryErr != nil {
				router.log().Error(ctx, fmt.Sprintf("error when run cron discovery: %s", discoveryErr))
			}
		}()

		router.cancelDiscovery = cancelFunc
	}

	nWorkers := int32(2)
	if cfg.NWorkers != 0 {
		nWorkers = cfg.NWorkers
	}

	router.nWorkers = nWorkers

	return router, err
}

// BucketSet Set a bucket to a replicaset.
func (r *Router) BucketSet(bucketID uint64, rsID uuid.UUID) (*Replicaset, error) {
	rs := r.idToReplicaset[rsID]
	if rs == nil {
		return nil, Errors[9] // NO_ROUTE_TO_BUCKET
	}

	oldReplicaset := r.routeMap[bucketID]

	if oldReplicaset != rs {
		if oldReplicaset != nil {
			oldReplicaset.bucketCount.Add(-1)
		} else {
			r.knownBucketCount.Add(1)
		}

		rs.bucketCount.Add(1)
	}

	r.routeMap[bucketID] = rs

	return rs, nil
}

func (r *Router) BucketReset(bucketID uint64) {
	if bucketID > uint64(len(r.routeMap))+1 {
		return
	}

	r.knownBucketCount.Add(-1)
	r.routeMap[bucketID] = nil
}

func (r *Router) RouteMapClean() {

	r.routeMap = make([]*Replicaset, r.cfg.TotalBucketCount+1)
	r.knownBucketCount.Store(0)

	for _, rs := range r.idToReplicaset {
		rs.bucketCount.Store(0)
	}

}

func prepareCfg(ctx context.Context, cfg Config) (Config, error) {
	err := validateCfg(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("%v: %v", ErrInvalidConfig, err)
	}

	if cfg.Logger == nil {
		cfg.Logger = &EmptyLogger{}
	}

	if cfg.Metrics == nil {
		cfg.Metrics = &EmptyMetrics{}
	}

	return cfg, nil
}

func validateCfg(cfg Config) error {
	if cfg.TopologyProvider == nil {
		return fmt.Errorf("topology provider is nil")
	}
	if cfg.TotalBucketCount == 0 {
		return fmt.Errorf("bucket count must be grather then 0")
	}

	return nil
}

// --------------------------------------------------------------------------------
// -- Other
// --------------------------------------------------------------------------------

// RouterBucketID  return the bucket identifier from the parameter used for sharding
// Deprecated: RouterBucketID() is deprecated, use RouterBucketIDStrCRC32() RouterBucketIDMPCRC32() instead
func (r *Router) RouterBucketID(shardKey string) uint64 {
	return BucketIDStrCRC32(shardKey, r.cfg.TotalBucketCount)
}

func BucketIDStrCRC32(shardKey string, totalBucketCount uint64) uint64 {
	return crc.CalculateCRC(&crc.Parameters{
		Width:      32,
		Polynomial: 0x1EDC6F41,
		FinalXor:   0x0,
		ReflectIn:  true,
		ReflectOut: true,
		Init:       0xFFFFFFFF,
	}, []byte(shardKey))%totalBucketCount + 1
}

func (r *Router) RouterBucketIDStrCRC32(shardKey string) uint64 {
	return BucketIDStrCRC32(shardKey, r.cfg.TotalBucketCount)
}

// RouterBucketIDMPCRC32 is not supported now
func RouterBucketIDMPCRC32(total uint64, keys ...string) {}

func (r *Router) RouterBucketCount() uint64 {
	return r.cfg.TotalBucketCount
}

// todo: router_sync

// --------------------------------------------------------------------------------
// -- Public API protection
// --------------------------------------------------------------------------------

// todo: router_api_call_safe
// todo: router_api_call_unsafe
// todo: router_make_api
// todo: router_enable
// todo: router_disable

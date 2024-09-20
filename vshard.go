package vshard_router //nolint:revive

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

var (
	ErrInvalidConfig       = fmt.Errorf("config invalid")
	ErrInvalidInstanceInfo = fmt.Errorf("invalid instance info")
	ErrTopologyProvider    = fmt.Errorf("got error from topology provider")
)

// This data struct is instroduced by https://github.com/KaymeKaydex/go-vshard-router/issues/39.
// We use an array of atomics to lock-free handling elements of routeMap.
// knownBucketCount reflects a statistic over routeMap.
// knownBucketCount might be inconsistent for a few mksecs, because at first we change routeMap[bucketID],
// only after that we change knownBucketCount: this is not an atomic change of complex state.
// It it is not a problem at all.
//
// While changing `knownBucketCount` we heavily rely on commutative property of algebraic sum operation ("+"),
// due to this property we don't afraid any amount of concurrent modifications.
// See: https://en.wikipedia.org/wiki/Commutative_property
//
// Since RouteMapClean creates a new routeMap, we have to assign knownBucketCount := 0.
// But assign is not a commutative operation, therefore we have to create a completely new atomic variable,
// that reflects a statistic over newly created routeMap.
type consistentView struct {
	routeMap         []atomic.Pointer[Replicaset]
	knownBucketCount atomic.Int32
}

type Router struct {
	cfg Config

	// idToReplicasetMutex guards not the map itself, but the variable idToReplicaset.
	// idToReplicaset is an immutable object by our convention.
	// Whenever we add or remove a replicaset, we create a new map object.
	// idToReplicaset can be modified only by TopologyController methods.
	// Assuming that we rarely add or remove some replicaset,
	// it should be the simplest and most efficient way of handling concurrent access.
	// Additionally, we can safely iterate over a map because it never changes.
	idToReplicasetMutex sync.RWMutex
	idToReplicaset      map[uuid.UUID]*Replicaset

	viewMutex sync.RWMutex
	view      *consistentView

	// ----------------------- Map-Reduce -----------------------
	// Storage Ref ID. It must be unique for each ref request
	// and therefore is global and monotonically growing.
	refID atomic.Int64

	cancelDiscovery func()
}

func (r *Router) metrics() MetricsProvider {
	return r.cfg.Metrics
}

func (r *Router) log() LogfProvider {
	return r.cfg.Loggerf
}

func (r *Router) getConsistentView() *consistentView {
	r.viewMutex.RLock()
	view := r.view
	r.viewMutex.RUnlock()

	return view
}

func (r *Router) setConsistentView(view *consistentView) {
	r.viewMutex.Lock()
	r.view = view
	r.viewMutex.Unlock()
}

type Config struct {
	// Providers
	Logger           LogProvider      // Logger is not required, legacy interface
	Loggerf          LogfProvider     // Loggerf is not required, new interface
	Metrics          MetricsProvider  // Metrics is not required
	TopologyProvider TopologyProvider // TopologyProvider is required provider

	// Discovery
	// DiscoveryTimeout is timeout between cron discovery job; by default there is no timeout.
	DiscoveryTimeout time.Duration
	DiscoveryMode    DiscoveryMode

	TotalBucketCount uint64
	User             string
	Password         string
	PoolOpts         tarantool.Opts

	// BucketGetter is an optional argument.
	// You can specify a function that will receive the bucket id from the context.
	// This is useful if you use middleware that inserts the calculated bucket id into the request context.
	BucketGetter func(ctx context.Context) uint64
	// RequestTimeout timeout for requests to Tarantool.
	// Don't rely on using this timeout.
	// Currently, it only works for sugar implementations and works as a retry timeout.
	RequestTimeout time.Duration
}

type BucketStatInfo struct {
	BucketID uint64 `mapstructure:"id"`
	Status   string `mapstructure:"status"`
}

type InstanceInfo struct {
	// Name is human-readable id for instance
	// Starting with tarantool 3.0, the definition is made into a human-readable name,
	// so far it is not used directly inside the library
	Name string
	Addr string
	UUID uuid.UUID
}

func (ii InstanceInfo) Validate() error {
	if ii.UUID == uuid.Nil {
		return fmt.Errorf("%w: empty uuid", ErrInvalidInstanceInfo)
	}

	if ii.Addr == "" {
		return fmt.Errorf("%w: empty addr", ErrInvalidInstanceInfo)
	}

	return nil
}

// --------------------------------------------------------------------------------
// -- Configuration
// --------------------------------------------------------------------------------

func NewRouter(ctx context.Context, cfg Config) (*Router, error) {
	var err error

	cfg, err = prepareCfg(cfg)
	if err != nil {
		return nil, err
	}

	router := &Router{
		cfg:            cfg,
		idToReplicaset: make(map[uuid.UUID]*Replicaset),
		view: &consistentView{
			routeMap: make([]atomic.Pointer[Replicaset], cfg.TotalBucketCount+1),
		},
	}

	err = cfg.TopologyProvider.Init(router.Topology())
	if err != nil {
		router.log().Errorf(ctx, "cant create new topology provider with err: %s", err)

		return nil, fmt.Errorf("%w; cant init topology with err: %w", ErrTopologyProvider, err)
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
				router.log().Errorf(ctx, "error when run cron discovery: %s", discoveryErr)
			}
		}()

		router.cancelDiscovery = cancelFunc
	}

	return router, err
}

// BucketSet Set a bucket to a replicaset.
func (r *Router) BucketSet(bucketID uint64, rsID uuid.UUID) (*Replicaset, error) {
	idToReplicasetRef := r.getIDToReplicaset()

	rs := idToReplicasetRef[rsID]
	if rs == nil {
		return nil, Errors[9] // NO_ROUTE_TO_BUCKET
	}

	view := r.getConsistentView()

	oldReplicaset := view.routeMap[bucketID].Swap(rs)
	if oldReplicaset != rs {
		if oldReplicaset != nil {
			oldReplicaset.bucketCount.Add(-1)
		} else {
			view.knownBucketCount.Add(1)
		}

		rs.bucketCount.Add(1)
	}

	return rs, nil
}

func (r *Router) BucketReset(bucketID uint64) {
	view := r.getConsistentView()

	if bucketID > uint64(len(view.routeMap))+1 {
		return
	}

	if old := view.routeMap[bucketID].Swap(nil); old != nil {
		view.knownBucketCount.Add(-1)
	}
}

func (r *Router) RouteMapClean() {
	idToReplicasetRef := r.getIDToReplicaset()

	newView := &consistentView{
		routeMap: make([]atomic.Pointer[Replicaset], r.cfg.TotalBucketCount+1),
	}

	r.setConsistentView(newView)

	for _, rs := range idToReplicasetRef {
		rs.bucketCount.Store(0)
	}
}

func prepareCfg(cfg Config) (Config, error) {
	err := validateCfg(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("%v: %v", ErrInvalidConfig, err)
	}

	if cfg.Loggerf == nil {
		if cfg.Logger != nil {
			cfg.Loggerf = &legacyLoggerProxy{l: cfg.Logger}
		} else {
			cfg.Loggerf = emptyLogfProvider
		}
	}

	if cfg.Metrics == nil {
		cfg.Metrics = emptyMetricsProvider
	}

	return cfg, nil
}

func validateCfg(cfg Config) error {
	if cfg.TopologyProvider == nil {
		return fmt.Errorf("topology provider is nil")
	}

	if cfg.TotalBucketCount == 0 {
		return fmt.Errorf("bucket count must be greater than 0")
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

// RouterBucketIDMPCRC32 is not implemented yet
func RouterBucketIDMPCRC32(total uint64, keys ...string) {
	// todo: implement
	_, _ = total, keys
	panic("RouterBucketIDMPCRC32 is not implemented yet")
}

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

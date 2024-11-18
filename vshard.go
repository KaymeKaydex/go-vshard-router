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
	// Loggerf injects a custom logger. By default there is no logger is used.
	Loggerf          LogfProvider     // Loggerf is not required
	Metrics          MetricsProvider  // Metrics is not required
	TopologyProvider TopologyProvider // TopologyProvider is required provider

	// Discovery
	// DiscoveryTimeout is timeout between cron discovery job; by default there is no timeout.
	DiscoveryTimeout time.Duration
	DiscoveryMode    DiscoveryMode
	// DiscoveryWorkStep is a pause between calling buckets_discovery on storage
	// in buckets discovering logic. Default is 10ms.
	DiscoveryWorkStep time.Duration

	// BucketsSearchMode defines policy for BucketDiscovery method.
	// Default value is BucketsSearchLegacy.
	// See BucketsSearchMode constants for more detail.
	BucketsSearchMode BucketsSearchMode

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
	// This is the difference between the timeout of the library itself
	// that is, our retry timeout if the buckets, for example, move.
	// Currently, it only works for sugar implementations .
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

func (ii InstanceInfo) String() string {
	return fmt.Sprintf("{name: %s, uuid: %s, addr: %s}", ii.Name, ii.UUID, ii.Addr)
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

	cfg, err = prepareCfg(ctx, cfg)
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
		discoveryCronCtx, cancelFunc := context.WithCancel(ctx)

		// run background cron discovery loop
		// suppress linter warning: Non-inherited new context, use function like `context.WithXXX` instead (contextcheck)
		//nolint:contextcheck
		go router.cronDiscovery(discoveryCronCtx)

		router.cancelDiscovery = cancelFunc
	}

	return router, err
}

// BucketSet Set a bucket to a replicaset.
func (r *Router) BucketSet(bucketID uint64, rsID uuid.UUID) (*Replicaset, error) {
	idToReplicasetRef := r.getIDToReplicaset()

	rs := idToReplicasetRef[rsID]
	if rs == nil {
		return nil, newVShardErrorNoRouteToBucket(bucketID)
	}

	view := r.getConsistentView()

	if oldRs := view.routeMap[bucketID].Swap(rs); oldRs == nil {
		view.knownBucketCount.Add(1)
	}

	return rs, nil
}

func (r *Router) BucketReset(bucketID uint64) {
	view := r.getConsistentView()

	if bucketID > r.cfg.TotalBucketCount {
		return
	}

	if old := view.routeMap[bucketID].Swap(nil); old != nil {
		view.knownBucketCount.Add(-1)
	}
}

func (r *Router) RouteMapClean() {
	newView := &consistentView{
		routeMap: make([]atomic.Pointer[Replicaset], r.cfg.TotalBucketCount+1),
	}

	r.setConsistentView(newView)
}

func prepareCfg(ctx context.Context, cfg Config) (Config, error) {
	const discoveryTimeoutDefault = 1 * time.Minute
	const discoveryWorkStepDefault = 10 * time.Millisecond

	err := validateCfg(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("%v: %v", ErrInvalidConfig, err)
	}

	if cfg.DiscoveryTimeout == 0 {
		cfg.DiscoveryTimeout = discoveryTimeoutDefault
	}

	if cfg.Loggerf == nil {
		cfg.Loggerf = emptyLogfProvider
	}

	// Log tarantool internal events using the same logger as router uses.
	cfg.PoolOpts.Logger = tarantoolOptsLogger{
		loggerf: cfg.Loggerf,
		ctx:     ctx,
	}

	if cfg.Metrics == nil {
		cfg.Metrics = emptyMetricsProvider
	}

	if cfg.DiscoveryWorkStep == 0 {
		cfg.DiscoveryWorkStep = discoveryWorkStepDefault
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

// -------------------------------------------------------------------------------_
// -- Bootstrap
// --------------------------------------------------------------------------------

// ClusterBootstrap initializes the cluster by bootstrapping the necessary buckets
// across the available replicasets. It checks the current state of each replicaset
// and creates buckets if required. The function takes a context for managing
// cancellation and deadlines, and a boolean parameter ifNotBootstrapped to control
// error handling. If ifNotBootstrapped is true, the function will log any errors
// encountered during the bootstrapping process but will not halt execution; instead,
// it will return the last error encountered. If ifNotBootstrapped is false, any
// error will result in an immediate return, ensuring that the operation either
// succeeds fully or fails fast.
func (r *Router) ClusterBootstrap(ctx context.Context, ifNotBootstrapped bool) error {
	rssToBootstrap := make([]Replicaset, 0, len(r.idToReplicaset))
	var lastErr error

	for _, rs := range r.idToReplicaset {
		rssToBootstrap = append(rssToBootstrap, *rs)
	}

	err := CalculateEtalonBalance(rssToBootstrap, r.cfg.TotalBucketCount)
	if err != nil {
		return err
	}

	bucketID := uint64(1)
	for id, rs := range rssToBootstrap {
		if rs.EtalonBucketCount > 0 {
			err = rs.BucketForceCreate(ctx, bucketID, rs.EtalonBucketCount)
			if err != nil {
				if ifNotBootstrapped {
					lastErr = err
				} else {
					return err
				}
			} else {
				nextBucketID := bucketID + rs.EtalonBucketCount
				r.log().Infof(ctx, "Buckets from %d to %d are bootstrapped on \"%s\"", bucketID, nextBucketID-1, id)
				bucketID = nextBucketID
			}
		}
	}

	if lastErr != nil {
		return lastErr
	}

	return nil
}

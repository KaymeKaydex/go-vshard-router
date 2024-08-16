package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

var (
	ErrReplicasetExists    = fmt.Errorf("replicaset exists")
	ErrReplicasetNotExists = fmt.Errorf("replicaset not exists")
)

// TopologyController is an entity that allows you to interact with the topology.
// TopologyController is not concurrent safe.
// This decision is made intentionally because there is no point in providing concurrence safety for this case.
// In any case, a caller can use his own external synchronization primitive to handle concurrent access.
type TopologyController interface {
	AddInstance(ctx context.Context, rsID uuid.UUID, info InstanceInfo) error
	RemoveReplicaset(ctx context.Context, rsID uuid.UUID) []error
	RemoveInstance(ctx context.Context, rsID, instanceID uuid.UUID) error
	AddReplicaset(ctx context.Context, rsInfo ReplicasetInfo, instances []InstanceInfo) error
	AddReplicasets(ctx context.Context, replicasets map[ReplicasetInfo][]InstanceInfo) error
}

func (r *Router) getIDToReplicaset() map[uuid.UUID]*Replicaset {
	r.idToReplicasetMutex.RLock()
	idToReplicasetRef := r.idToReplicaset
	r.idToReplicasetMutex.RUnlock()

	return idToReplicasetRef
}

func (r *Router) setIDToReplicaset(idToReplicasetNew map[uuid.UUID]*Replicaset) {
	r.idToReplicasetMutex.Lock()
	r.idToReplicaset = idToReplicasetNew
	r.idToReplicasetMutex.Unlock()
}

func (r *Router) Topology() TopologyController {
	return r
}

func (r *Router) AddInstance(ctx context.Context, rsID uuid.UUID, info InstanceInfo) error {
	err := info.Validate()
	if err != nil {
		return err
	}

	instance := pool.Instance{
		Name: info.UUID.String(),
		Dialer: tarantool.NetDialer{
			Address:  info.Addr,
			User:     r.cfg.User,
			Password: r.cfg.Password,
		},
	}

	idToReplicasetRef := r.getIDToReplicaset()

	rs := idToReplicasetRef[rsID]
	if rs == nil {
		return ErrReplicasetNotExists
	}

	return rs.conn.Add(ctx, instance)
}

func (r *Router) RemoveInstance(_ context.Context, rsID, instanceID uuid.UUID) error {
	idToReplicasetRef := r.getIDToReplicaset()

	rs := idToReplicasetRef[rsID]
	if rs == nil {
		return ErrReplicasetNotExists
	}

	return rs.conn.Remove(instanceID.String())
}

func (r *Router) AddReplicaset(ctx context.Context, rsInfo ReplicasetInfo, instances []InstanceInfo) error {
	idToReplicasetOld := r.getIDToReplicaset()

	if _, ok := idToReplicasetOld[rsInfo.UUID]; ok {
		return ErrReplicasetExists
	}

	replicaset := &Replicaset{
		info: ReplicasetInfo{
			Name: rsInfo.Name,
			UUID: rsInfo.UUID,
		},
		// according to the documentation, it will be initialized by zero, see: https://pkg.go.dev/sync/atomic#Int32
		bucketCount: atomic.Int32{},
	}

	rsInstances := make([]pool.Instance, 0, len(instances))
	for _, instance := range instances {
		rsInstances = append(rsInstances, pool.Instance{
			Name: instance.UUID.String(),
			Dialer: tarantool.NetDialer{
				Address:  instance.Addr,
				User:     r.cfg.User,
				Password: r.cfg.Password,
			},
			Opts: r.cfg.PoolOpts,
		})
	}

	conn, err := pool.Connect(ctx, rsInstances)
	if err != nil {
		return err
	}

	isConnected, err := conn.ConnectedNow(pool.RW)
	if err != nil {
		return fmt.Errorf("cant check rs pool conntected rw now with error: %s", err)
	}

	if !isConnected {
		return fmt.Errorf("got connected now as false, storage must be configured first")
	}

	replicaset.conn = conn

	// Create an entirely new map object
	idToReplicasetNew := make(map[uuid.UUID]*Replicaset)
	for k, v := range idToReplicasetOld {
		idToReplicasetNew[k] = v
	}
	idToReplicasetNew[rsInfo.UUID] = replicaset // add when conn is ready

	// We could detect concurrent access to the TopologyController interface
	// by comparing references to r.idToReplicaset and idToReplicasetOld.
	// But it requires reflection which I prefer to avoid.
	// See: https://stackoverflow.com/questions/58636694/how-to-know-if-2-go-maps-reference-the-same-data.
	r.setIDToReplicaset(idToReplicasetNew)

	return nil
}

func (r *Router) AddReplicasets(ctx context.Context, replicasets map[ReplicasetInfo][]InstanceInfo) error {
	for rsInfo, rsInstances := range replicasets {
		// We assume that AddReplicasets is called only once during initialization.
		// We also expect that cluster configuration changes very rarely,
		// so we prefer more simple code rather than the efficiency of this part of logic.
		// Even if there are 1000 replicasets, it is still cheap.
		err := r.AddReplicaset(ctx, rsInfo, rsInstances)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Router) RemoveReplicaset(_ context.Context, rsID uuid.UUID) []error {
	idToReplicasetOld := r.getIDToReplicaset()

	rs := idToReplicasetOld[rsID]
	if rs == nil {
		return []error{ErrReplicasetNotExists}
	}

	// Create an entirely new map object
	idToReplicasetNew := make(map[uuid.UUID]*Replicaset)
	for k, v := range idToReplicasetOld {
		idToReplicasetNew[k] = v
	}
	delete(idToReplicasetNew, rsID)

	r.setIDToReplicaset(idToReplicasetNew)

	return rs.conn.CloseGraceful()
}

package vshard_router

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

var ErrReplicasetNotExists = fmt.Errorf("replicaset not exists")

type TopologyController interface {
	AddInstance(ctx context.Context, rsID uuid.UUID, info InstanceInfo) error
	RemoveReplicaset(ctx context.Context, rsID uuid.UUID) []error
	RemoveInstance(ctx context.Context, rsID, instanceID uuid.UUID) error
	AddReplicaset(ctx context.Context, rsInfo ReplicasetInfo, instances []InstanceInfo) error
	AddReplicasets(ctx context.Context, replicasets map[ReplicasetInfo][]InstanceInfo) error
}

// TopologyController is an entity that allows you to interact with the topology
type controller struct {
	r *Router
}

func (r *Router) Topology() TopologyController {
	return &controller{r: r}
}

func (c *controller) AddInstance(ctx context.Context, rsID uuid.UUID, info InstanceInfo) error {
	err := info.Validate()
	if err != nil {
		return err
	}

	instance := pool.Instance{
		Name: info.UUID.String(),
		Dialer: tarantool.NetDialer{
			Address:  info.Addr,
			User:     c.r.cfg.User,
			Password: c.r.cfg.Password,
		},
	}

	rs := c.r.idToReplicaset[rsID]
	if rs == nil {
		return ErrReplicasetNotExists
	}

	return rs.conn.Add(ctx, instance)
}

func (c *controller) RemoveInstance(_ context.Context, rsID, instanceID uuid.UUID) error {
	rs := c.r.idToReplicaset[rsID]
	if rs == nil {
		return ErrReplicasetNotExists
	}

	return rs.conn.Remove(instanceID.String())
}

func (c *controller) AddReplicaset(ctx context.Context, rsInfo ReplicasetInfo, instances []InstanceInfo) error {
	router := c.r
	cfg := router.cfg

	replicaset := &Replicaset{
		info: ReplicasetInfo{
			Name: rsInfo.Name,
			UUID: rsInfo.UUID,
		},
		bucketCount: atomic.Int32{},
	}

	replicaset.bucketCount.Store(0)

	rsInstances := make([]pool.Instance, len(instances))

	for i, instance := range instances {
		dialer := tarantool.NetDialer{
			Address:  instance.Addr,
			User:     cfg.User,
			Password: cfg.Password,
		}
		inst := pool.Instance{
			Name:   instance.UUID.String(),
			Dialer: dialer,
			Opts:   router.cfg.PoolOpts,
		}

		rsInstances[i] = inst
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
	router.idToReplicaset[rsInfo.UUID] = replicaset // add when conn is ready

	return nil
}

func (c *controller) AddReplicasets(ctx context.Context, replicasets map[ReplicasetInfo][]InstanceInfo) error {
	for rsInfo, rsInstances := range replicasets {
		err := c.AddReplicaset(ctx, rsInfo, rsInstances)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *controller) RemoveReplicaset(ctx context.Context, rsID uuid.UUID) []error {
	r := c.r

	rs := r.idToReplicaset[rsID]
	if rs == nil {
		return []error{ErrReplicasetNotExists}
	}

	errors := rs.conn.CloseGraceful()
	delete(r.idToReplicaset, rsID)

	return errors
}

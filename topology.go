package vshard_router

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

// TopologyController is an entity that allows you to interact with the topology
type TopologyController struct {
	r *Router
}

func (r *Router) Topology() *TopologyController {
	return &TopologyController{r: r}
}

func (t *TopologyController) AddInstance(ctx context.Context, rsID uuid.UUID, info InstanceInfo) error {
	instance := pool.Instance{
		Name: info.UUID.String(),
		Dialer: tarantool.NetDialer{
			Address:  info.Addr,
			User:     t.r.cfg.User,
			Password: t.r.cfg.Password,
		},
	}
	return t.r.idToReplicaset[rsID].conn.Add(ctx, instance)
}

func (t *TopologyController) RemoveInstance(ctx context.Context, rsID, instanceID uuid.UUID) error {
	return t.r.idToReplicaset[rsID].conn.Remove(instanceID.String())
}

func (t *TopologyController) AddReplicaset(ctx context.Context, rsInfo ReplicasetInfo, instances []InstanceInfo) error {
	router := t.r
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

func (t *TopologyController) AddReplicasets(ctx context.Context, replicasets map[ReplicasetInfo][]InstanceInfo) error {
	for rsInfo, rsInstances := range replicasets {
		err := t.AddReplicaset(ctx, rsInfo, rsInstances)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TopologyController) RemoveReplicaset(ctx context.Context, rsID uuid.UUID) []error {
	r := t.r

	errors := r.idToReplicaset[rsID].conn.CloseGraceful()
	delete(r.idToReplicaset, rsID)

	return errors
}

package viper

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	srcviper "github.com/spf13/viper"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
)

type Provider struct {
	rs map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo
}

func NewProvider(v *srcviper.Viper) *Provider {
	if v == nil {
		panic("viper entity is nil")
	}

	return &Provider{rs: rs}
}

func (p *Provider) WatchChanges() *Provider {
	// todo
	return p
}

func (p *Provider) Validate() error {
	if len(p.rs) < 1 {
		return fmt.Errorf("replicasets are empty")
	}

	for rs := range p.rs {
		// check replicaset name
		if rs.Name == "" {
			return fmt.Errorf("one of replicaset name is empty")
		}

		// check replicaset uuid
		if rs.UUID == uuid.Nil {
			return fmt.Errorf("one of replicaset uuid is empty")
		}
	}

	return nil
}

func (p *Provider) Init(c *vshardrouter.TopologyController) error {
	return c.AddReplicasets(context.TODO(), p.rs)
}

func (p *Provider) Close() {}

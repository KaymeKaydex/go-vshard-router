package viper

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	srcviper "github.com/spf13/viper"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
)

type Provider struct {
	v  *srcviper.Viper
	rs map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo
}

func NewProvider(v *srcviper.Viper) *Provider {
	if v == nil {
		panic("viper entity is nil")
	}

	v.Unmarshal()
	return &Provider{v: v}
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

func (p *Provider) Close() {
	return
}

type ClusterInfo struct {
	ReplicasetUUID string `yaml:"replicaset_uuid" mapstructure:"replicaset_uuid"`
}

type InstanceInfo struct {
	Cluster string
	Box     struct {
		Listen       string `json:"listen,omitempty" yaml:"listen" mapstructure:"listen"`
		InstanceUUID string `yaml:"instance_uuid" mapstructure:"instance_uuid" json:"instanceUUID,omitempty"`
	}
}
type SourceTopologyConfig struct {
	Clusters  map[string]ClusterInfo  `json:"clusters,omitempty" yaml:"clusters" `
	Instances map[string]InstanceInfo `json:"instances,omitempty" yaml:"instances"`
}

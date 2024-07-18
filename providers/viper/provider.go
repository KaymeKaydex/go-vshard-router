package viper

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	srcviper "github.com/spf13/viper"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
)

// Check that provider implements TopologyProvider interface
var _ vshardrouter.TopologyProvider = (*Provider)(nil)

type Provider struct {
	v  *srcviper.Viper
	rs map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo
}

func NewProvider(v *srcviper.Viper) *Provider {
	if v == nil {
		panic("viper entity is nil")
	}

	cfg := &TopologyConfig{}
	err := v.Unmarshal(cfg)
	if err != nil {
		panic(err)
	}

	if cfg.Topology.Instances == nil {
		panic("instances is nil")
	}

	if cfg.Topology.Clusters == nil {
		panic("clusters is nil")
	}

	// готовим конфиг для vshard-router`а
	vshardRouterTopology := make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)

	for rsName, rs := range cfg.Topology.Clusters {
		rsUUID, err := uuid.Parse(rs.ReplicasetUUID)
		if err != nil {
			log.Printf("cant parse replicaset uuid: %s", err)

			os.Exit(2)
		}

		rsInstances := make([]vshardrouter.InstanceInfo, 0)

		for _, instInfo := range cfg.Topology.Instances {
			if instInfo.Cluster != rsName {
				continue
			}

			instUUID, err := uuid.Parse(instInfo.Box.InstanceUUID)
			if err != nil {
				log.Printf("cant parse replicaset uuid: %s", err)

				panic(err)
			}

			rsInstances = append(rsInstances, vshardrouter.InstanceInfo{
				Addr: instInfo.Box.Listen,
				UUID: instUUID,
			})
		}

		vshardRouterTopology[vshardrouter.ReplicasetInfo{
			Name: rsName,
			UUID: rsUUID,
		}] = rsInstances
	}

	return &Provider{v: v, rs: vshardRouterTopology}
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

func (p *Provider) Init(c vshardrouter.TopologyController) error {
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

type TopologyConfig struct {
	Topology SourceTopologyConfig `json:"topology"`
}

type SourceTopologyConfig struct {
	Clusters  map[string]ClusterInfo  `json:"clusters,omitempty" yaml:"clusters" `
	Instances map[string]InstanceInfo `json:"instances,omitempty" yaml:"instances"`
}

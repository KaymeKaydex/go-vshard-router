package etcd

import (
	"context"
	"fmt"
	"log"
	"path/filepath"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/google/uuid"
	"go.etcd.io/etcd/client/v2"
)

// Check that provider implements TopologyProvider interface
var _ vshardrouter.TopologyProvider = (*Provider)(nil)

type Provider struct {
	kapi client.KeysAPI
	path string
}

type Config struct {
	EtcdConfig client.Config
	// Path for storages configuration in etcd for example /project/store/storage
	Path string
}

// NewProvider returns provider to etcd configuration
// Set here path to etcd storages config and etcd config
func NewProvider(cfg Config) *Provider {
	c, err := client.New(cfg.EtcdConfig)
	if err != nil {
		log.Fatal(err)
	}

	kapi := client.NewKeysAPI(c)

	return &Provider{
		kapi: kapi,
		path: cfg.Path,
	}
}

// mapCluster2Instances combines clusters with instances in map
func mapCluster2Instances(replicasets []vshardrouter.ReplicasetInfo,
	instances map[string][]*vshardrouter.InstanceInfo) map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo {

	currentTopology := map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo{}

	for _, replicasetInfo := range replicasets {
		var resInst []vshardrouter.InstanceInfo

		for _, inst := range instances[replicasetInfo.Name] {
			resInst = append(resInst, *inst)
		}

		currentTopology[replicasetInfo] = resInst
	}

	return currentTopology
}

func (p *Provider) GetTopology() (map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo, error) {
	resp, err := p.kapi.Get(context.TODO(), p.path, &client.GetOptions{Recursive: true})
	if err != nil {
		return nil, err
	}
	nodes := resp.Node.Nodes

	if nodes.Len() < 2 {
		return nil, fmt.Errorf("etcd path %s subnodes <2; minimum 2 (/clusters & /instances)", p.path)
	}

	var replicasets []vshardrouter.ReplicasetInfo
	instances := map[string][]*vshardrouter.InstanceInfo{} // cluster name to instance info

	for _, node := range nodes {
		var err error

		switch filepath.Base(node.Key) {
		case "clusters":
			if len(node.Nodes) < 1 {
				return nil, fmt.Errorf("etcd path %s has no clusters", node.Key)
			}

			for _, rsNode := range node.Nodes {
				replicaset := vshardrouter.ReplicasetInfo{}

				replicaset.Name = filepath.Base(rsNode.Key)

				for _, rsInfoNode := range rsNode.Nodes {
					switch filepath.Base(rsInfoNode.Key) {
					case "replicaset_uuid":
						replicaset.UUID, err = uuid.Parse(rsInfoNode.Value)
						if err != nil {
							return nil, fmt.Errorf("cant parse replicaset %s uuid %s", replicaset.Name, rsInfoNode.Value)
						}
					case "master":
						// TODO: now we dont support non master auto implementation
					default:
						continue
					}
				}

				replicasets = append(replicasets, replicaset)
			}
		case "instances":
			if len(node.Nodes) < 1 {
				return nil, fmt.Errorf("etcd path %s has no instances", node.Key)
			}

			for _, instanceNode := range node.Nodes {
				instanceName := filepath.Base(instanceNode.Key)

				instance := &vshardrouter.InstanceInfo{
					Name: instanceName,
				}

				for _, instanceInfoNode := range instanceNode.Nodes {
					switch filepath.Base(instanceInfoNode.Key) {
					case "cluster":
						instances[instanceInfoNode.Value] = append(instances[instanceInfoNode.Value], instance)
					case "box":
						for _, boxNode := range instanceInfoNode.Nodes {
							switch filepath.Base(boxNode.Key) {
							case "listen":
								instance.Addr = boxNode.Value
							case "instance_uuid":
								instance.UUID, err = uuid.Parse(boxNode.Value)
								if err != nil {
									return nil, fmt.Errorf("cant parse for instance uuid %s", boxNode.Value)
								}
							}
						}
					}
				}
			}
		default:
			continue
		}
	}

	currentTopology := mapCluster2Instances(replicasets, instances)

	return currentTopology, nil
}

func (p *Provider) Init(c vshardrouter.TopologyController) error {
	topology, err := p.GetTopology()
	if err != nil {
		return err
	}

	return c.AddReplicasets(context.TODO(), topology)
}

// Close must close connection, but etcd v2 client has no interfaces for this
func (p *Provider) Close() {}

package main

import (
	"log"
	"net/http"
	"os"
	"time"

	vshardrouter "github.com/KaymeKaydex/go-vhsard-router"
)

type Config struct {
	ListenerConfig ListenerConfig `yaml:"listener" mapstructure:"listener"`
	Routers        RoutersConfig  `yaml:"routers"`
	Storage        StorageConfig  `yaml:"storage" mapstructure:"storage"`
}

type ListenerConfig struct {
	Address string `yaml:"address" mapstructure:"address"`
}

type RoutersConfig struct {
	Addrs []string `yaml:"addrs" mapstructure:"addrs"`
}

type StorageConfig struct {
	TotalBucketCount uint64                                                      `yaml:"total_bucket_count" mapstructure:"total_bucket_count"`
	SourceTopology   SourceTopologyConfig                                        `yaml:"topology,omitempty" mapstructure:"topology,omitempty"`
	Topology         map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo `yaml:"-" mapstructure:"-"`
}
type ClusterInfo struct {
	ReplicasetUUID string `yaml:"replicaset_uuid" mapstructure:"replicaset_uuid"`
}

type InstanceInfo struct {
	Cluster string
	Box     struct {
		Listen       string
		InstanceUUID string `yaml:"instance_uuid" mapstructure:"instance_uuid"`
	}
}
type SourceTopologyConfig struct {
	Clusters  map[string]ClusterInfo
	Instances map[string]InstanceInfo
}

func main() {
	var err error

	if len(os.Args) == 1 {
		log.Println("write config file path as argument")

		os.Exit(2)
	}

	mux := http.NewServeMux()
	log.Println("new mux server created")

	s := &http.Server{
		Addr:         ":8080",
		WriteTimeout: time.Minute,
		ReadTimeout:  time.Minute,
		IdleTimeout:  time.Minute,
		Handler:      mux,
	}

	err = s.ListenAndServe()
	if err != nil {
		log.Println(err)

		return
	}
}

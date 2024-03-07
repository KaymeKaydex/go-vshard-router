package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"

	vshardrouter "github.com/KaymeKaydex/go-vhsard-router"
)

func main() {
	ctx := context.Background()
	var err error

	if len(os.Args) == 1 {
		log.Println("write config file path as argument")

		os.Exit(2)
	}

	cfg := readCfg(os.Args[1])

	vshardRouter, err := vshardrouter.NewRouter(ctx, vshardrouter.Config{
		Logger:           &vshardrouter.StdoutLogger{},
		DiscoveryTimeout: time.Minute,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		Replicasets:      cfg.Storage.Topology,
		TotalBucketCount: cfg.Storage.TotalBucketCount,
		PoolOpts: tarantool.Opts{
			Timeout: time.Second,
		},
	})

	ctrl := controller{
		router: vshardRouter,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/customer_lookup", ctrl.CustomerLookupHandler)
	mux.HandleFunc("/customer_add", ctrl.CustomerAddHandler)

	log.Println("new mux server created")

	s := &http.Server{
		Addr:         cfg.ListenerConfig.Address,
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

// --------- HANDLERS ----------
type controller struct {
	router *vshardrouter.Router
}

type CustomerAddRequest struct {
	CustomerID int    `json:"customer_id" msgpack:"customer_id"`
	Name       string `json:"name" msgpack:"name"`
	BucketId   uint64 `json:"-" msgpack:"bucket_id"`
	Accounts   []struct {
		AccountId int    `json:"account_id" msgpack:"account_id"`
		Name      string `json:"name" msgpack:"name"`
	} `json:"accounts" msgpack:"accounts"`
}

func (c *controller) CustomerAddHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	req := &CustomerAddRequest{}

	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bucketID := c.router.RouterBucketIDStrCRC32(strconv.Itoa(req.CustomerID))

	req.BucketId = bucketID

	faces, _, err := c.router.RouterCallImpl(ctx, bucketID, vshardrouter.CallOpts{
		VshardMode: vshardrouter.WriteMode,
		PoolMode:   pool.RW,
		Timeout:    time.Minute,
	}, "customer_add", []interface{}{req})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Println(faces)
}

// customer lookup

type CustomerLookupResponse struct {
	Accounts []struct {
		AccountId int    `json:"account_id" msgpack:"account_id"`
		Balance   int    `json:"balance" msgpack:"balance"`
		Name      string `json:"name" msgpack:"name"`
	} `json:"accounts" msgpack:"accounts"`
	CustomerId int    `json:"customer_id" msgpack:"customer_id"`
	Name       string `json:"name" msgpack:"name"`
}

func (c *controller) CustomerLookupHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	customerID := r.URL.Query().Get("id") // get customer id
	if customerID == "" {
		http.Error(w, "no id query param", http.StatusInternalServerError)
		return
	}

	csID, err := strconv.Atoi(customerID)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bucketID := c.router.RouterBucketIDStrCRC32(customerID)
	_, typedFnc, err := c.router.RouterCallImpl(ctx, bucketID, vshardrouter.CallOpts{
		VshardMode: vshardrouter.ReadMode,
		PoolMode:   pool.PreferRO,
		Timeout:    time.Second,
	}, "customer_lookup", []interface{}{csID})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := &CustomerLookupResponse{}

	err = typedFnc(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

//
// --------- CONFIG ----------
//

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
	SourceTopology   *SourceTopologyConfig                                       `yaml:"topology,omitempty" mapstructure:"topology,omitempty"`
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

func readCfg(cfgPath string) Config {
	// read cfg
	viper.SetConfigType("yaml")
	viper.SetConfigFile(cfgPath)
	err := viper.ReadInConfig()
	if err != nil {
		log.Println("viper cant read in such config file ")
		os.Exit(2)
	}

	cfg := &Config{}
	err = viper.Unmarshal(cfg)
	if err != nil {
		log.Println(err)

		os.Exit(2)
	}

	// проверяем что если топология из конфига - то в конфиге она должна быть в наличии
	if cfg.Storage.SourceTopology == nil { // проверяем что из конфига эта топология спарсилась
		log.Println(fmt.Errorf("topology provider uses config source, but topology: is empty"))

		os.Exit(2)
	}

	// готовим конфиг для vshard-router`а
	vshardRouterTopology := make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)

	for rsName, rs := range cfg.Storage.SourceTopology.Clusters {
		rsUUID, err := uuid.Parse(rs.ReplicasetUUID)
		if err != nil {
			log.Printf("cant parse replicaset uuid: %s", err)

			os.Exit(2)
		}

		rsInstances := make([]vshardrouter.InstanceInfo, 0)

		for _, instInfo := range cfg.Storage.SourceTopology.Instances {
			if instInfo.Cluster != rsName {
				continue
			}

			instUUID, err := uuid.Parse(instInfo.Box.InstanceUUID)
			if err != nil {
				log.Printf("cant parse replicaset uuid: %s", err)

				os.Exit(2)
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
	cfg.Storage.Topology = vshardRouterTopology

	return *cfg
}

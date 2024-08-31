package tnt_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
)

const (
	totalBucketCount = 100

	envNreplicasetsKey = "NREPLICASETS"
	envStartPortKey    = "START_PORT"
)

func getEnvInt(key string) int {
	vStr := os.Getenv(key)

	v, err := strconv.Atoi(vStr)
	if err != nil {
		panic(err)
	}

	if v <= 0 {
		panic(fmt.Sprintf("ENV '%s' invalied: '%s'", key, vStr))
	}

	return v
}

func isCorrectRun() bool {
	if len(os.Getenv(envNreplicasetsKey)) == 0 || len(os.Getenv(envStartPortKey)) == 0 {
		return false
	}

	return true
}

func getCfg() map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo {
	c := cfgmaker{
		nreplicasets: getEnvInt(envNreplicasetsKey),
		startPort:    getEnvInt(envStartPortKey),
	}

	return c.clusterCfg()
}

func TestConcurrentRouterCall(t *testing.T) {
	/* TODO
	1) Invalidate some random bucket id
	2) concurrent call of replicalcall
	*/
	_ = t
}

func TestRetValues(t *testing.T) {
	/* TODO
	1) Replicacall returns no value, 1 value, 2 values, 3 values, etc..., assert, lua error?
	*/
	_ = t
}

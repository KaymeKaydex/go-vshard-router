package tnt

import (
	"fmt"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
	"github.com/google/uuid"
)

type cfgmaker struct {
	startPort    int
	nreplicasets int
}

func (c cfgmaker) getUUID(rsID int, n int) uuid.UUID {
	const uuidTemplate = "00000000-0000-%04d-%04d-000000000000"

	uuidStr := fmt.Sprintf(uuidTemplate, rsID, n)

	uuid, err := uuid.Parse(uuidStr)
	if err != nil {
		panic(err)
	}

	return uuid
}

func (c cfgmaker) replicasetUUID(rsID int) uuid.UUID {
	return c.getUUID(rsID, 0)
}

func (c cfgmaker) masterUUID(rsID int) uuid.UUID {
	return c.getUUID(rsID, 1)
}

func (c cfgmaker) followerUUID(rsID int) uuid.UUID {
	return c.getUUID(rsID, 2)
}

func (c cfgmaker) getInstanceAddr(port int) string {
	const addrTemplate = "127.0.0.1:%d"

	return fmt.Sprintf(addrTemplate, port)
}

func (c cfgmaker) masterAddr(rsID int) string {
	port := c.startPort + 2*(rsID-1)
	return c.getInstanceAddr(port)
}

func (c cfgmaker) followerAddr(rsID int) string {
	port := c.startPort + 2*(rsID-1) + 1
	return c.getInstanceAddr(port)
}

func (c cfgmaker) clusterCfg() map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo {
	cfg := make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)

	for rsID := 1; rsID <= c.nreplicasets; rsID++ {
		cfg[vshardrouter.ReplicasetInfo{
			Name: fmt.Sprintf("replicaset_%d", rsID),
			UUID: c.replicasetUUID(rsID),
		}] = []vshardrouter.InstanceInfo{{
			Addr: c.masterAddr(rsID),
			UUID: c.masterUUID(rsID),
		}, {
			Addr: c.followerAddr(rsID),
			UUID: c.followerUUID(rsID),
		}}
	}

	return cfg
}

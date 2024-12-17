package vshard_router //nolint:revive

import (
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

type UUIDToReplicasetMap map[uuid.UUID]*Replicaset

type routerConcurrentData interface {
	nextRefID() int64
	getRefs() (UUIDToReplicasetMap, *consistentView)
	setConsistentView(view *consistentView)
	setIDToReplicaset(idToReplicasetNew UUIDToReplicasetMap)
}

// routerConcurrentDataImpl is the router's data that can be accessed concurrently.
type routerConcurrentDataImpl struct {
	mutex sync.RWMutex

	// mutex guards not the map itself, but the variable idToReplicaset.
	// idToReplicaset is an immutable object by our convention.
	// Whenever we add or remove a replicaset, we create a new map object.
	// idToReplicaset variable can be modified only by setIDToReplicaset method.
	// Assuming that we rarely change idToReplicaset.
	// it should be the simplest and most efficient way of handling concurrent access.
	// Additionally, we can safely iterate over a map because it never changes.
	idToReplicaset UUIDToReplicasetMap
	// See comment for type consistentView.
	view *consistentView

	// ----------------------- Map-Reduce -----------------------
	// Storage Ref ID. It must be unique for each ref request
	// and therefore is global and monotonically growing.
	refID atomic.Int64
}

func newRouterConcurrentData(totalBucketCount uint64) routerConcurrentData {
	return &routerConcurrentDataImpl{
		idToReplicaset: make(UUIDToReplicasetMap),
		view: &consistentView{
			routeMap: make([]atomic.Pointer[Replicaset], totalBucketCount+1),
		},
	}
}

func (d *routerConcurrentDataImpl) nextRefID() int64 {
	return d.refID.Add(1)
}

func (d *routerConcurrentDataImpl) getRefs() (UUIDToReplicasetMap, *consistentView) {
	d.mutex.RLock()
	idToReplicasetRef, view := d.idToReplicaset, d.view
	d.mutex.RUnlock()

	return idToReplicasetRef, view
}

func (d *routerConcurrentDataImpl) setConsistentView(view *consistentView) {
	d.mutex.Lock()
	d.view = view
	d.mutex.Unlock()
}

func (d *routerConcurrentDataImpl) setIDToReplicaset(idToReplicasetNew UUIDToReplicasetMap) {
	d.mutex.Lock()
	d.idToReplicaset = idToReplicasetNew
	d.mutex.Unlock()
}

package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

// --------------------------------------------------------------------------------
// -- Discovery
// --------------------------------------------------------------------------------

type DiscoveryMode int

const (
	// DiscoveryModeOn is cron discovery with cron timeout
	DiscoveryModeOn DiscoveryMode = iota
	DiscoveryModeOnce
)

// BucketDiscovery search bucket in whole cluster
func (r *Router) BucketDiscovery(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	if bucketID < 1 || r.cfg.TotalBucketCount < bucketID {
		return nil, fmt.Errorf("bucket id is out of range: %d (total %d)", bucketID, r.cfg.TotalBucketCount)
	}

	view := r.getConsistentView()

	rs := view.routeMap[bucketID].Load()
	if rs != nil {
		return rs, nil
	}

	// it`s ok if in the same time we have few active searches
	r.log().Infof(ctx, "Discovering bucket %d", bucketID)

	idToReplicasetRef := r.getIDToReplicaset()

	type rsFuture struct {
		rsID   uuid.UUID
		future *tarantool.Future
	}

	var rsFutures = make([]rsFuture, 0, len(idToReplicasetRef))
	// Send a bunch of parallel requests
	for rsID, rs := range idToReplicasetRef {
		rsFutures = append(rsFutures, rsFuture{
			rsID:   rsID,
			future: rs.bucketStatAsync(ctx, bucketID),
		})
	}

	for _, rsFuture := range rsFutures {
		if _, err := bucketStatWait(rsFuture.future); err != nil {
			// just skip, bucket seems do not belong to this replicaset
			continue
		}

		// It's ok if several replicasets return ok to bucket_stat command for the same bucketID, just pick any of them.
		rs, err := r.BucketSet(bucketID, rsFuture.rsID)
		if err != nil {
			r.log().Errorf(ctx, "BucketDiscovery: can't set rsID %v for bucketID %d: %v", rsFuture.rsID, bucketID, err)
			return nil, Errors[9] // NO_ROUTE_TO_BUCKET
		}

		// TODO: should we release resources for unhandled futures?
		return rs, nil
	}

	/*
	   -- All replicasets were scanned, but a bucket was not
	   -- found anywhere, so most likely it does not exist. It
	   -- can be wrong, if rebalancing is in progress, and a
	   -- bucket was found to be RECEIVING on one replicaset, and
	   -- was not found on other replicasets (it was sent during
	   -- discovery).
	*/

	return nil, Errors[9] // NO_ROUTE_TO_BUCKET
}

// BucketResolve resolve bucket id to replicaset
func (r *Router) BucketResolve(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	return r.BucketDiscovery(ctx, bucketID)
}

// DiscoveryHandleBuckets arrange downloaded buckets to the route map so as they reference a given replicaset.
func (r *Router) DiscoveryHandleBuckets(ctx context.Context, rs *Replicaset, buckets []uint64) {
	view := r.getConsistentView()
	removedFrom := make(map[*Replicaset]int)

	for _, bucketID := range buckets {
		oldRs := view.routeMap[bucketID].Swap(rs)

		if oldRs == rs {
			continue
		}

		if oldRs == nil {
			view.knownBucketCount.Add(1)
		}

		// We don't check oldRs for nil here, because it's a valid key too (if rs == nil, it means removed from unknown buckets set)
		removedFrom[oldRs]++
	}

	var addedToRs int
	for rs, removedFromRs := range removedFrom {
		addedToRs += removedFromRs

		switch rs {
		case nil:
			r.log().Debugf(ctx, "Added new %d buckets to the cluster map", removedFromRs)
		default:
			r.log().Debugf(ctx, "Removed %d buckets from replicaset %s", removedFromRs, rs.info.Name)
		}
	}

	r.log().Infof(ctx, "Added %d buckets to replicaset %s", addedToRs, rs.info.Name)
}

func (r *Router) DiscoveryAllBuckets(ctx context.Context) error {
	t := time.Now()

	r.log().Infof(ctx, "start discovery all buckets")

	errGr, ctx := errgroup.WithContext(ctx)

	view := r.getConsistentView()
	idToReplicasetRef := r.getIDToReplicaset()

	for _, rs := range idToReplicasetRef {
		rs := rs

		errGr.Go(func() error {
			var bucketsDiscoveryPaginationRequest struct {
				From uint64 `msgpack:"from"`
			}

			for {
				req := tarantool.NewCallRequest("vshard.storage.buckets_discovery").
					Context(ctx).
					Args([]interface{}{&bucketsDiscoveryPaginationRequest})

				future := rs.conn.Do(req, pool.PreferRO)

				// We intentionally don't support old vshard storages that mentioned here:
				// https://github.com/tarantool/vshard/blob/8d299bfecff8bc656056658350ad48c829f9ad3f/vshard/router/init.lua#L343
				var resp struct {
					Buckets  []uint64 `msgpack:"buckets"`
					NextFrom uint64   `msgpack:"next_from"`
				}

				err := future.GetTyped(&[]interface{}{&resp})
				if err != nil {
					return err
				}

				for _, bucketID := range resp.Buckets {
					// We could check here that bucketID is in range [1, TotalBucketCnt], but it seems to be redundant.
					if old := view.routeMap[bucketID].Swap(rs); old == nil {
						view.knownBucketCount.Add(1)
					}
				}

				// There are no more buckets
				// https://github.com/tarantool/vshard/blob/8d299bfe/vshard/storage/init.lua#L1730
				// vshard.storage returns { buckets = [], next_from = nil } if there are no more buckets.
				// Since next_from is always > 0. NextFrom = 0 means that we got next_from = nil, that has not been decoded.
				if resp.NextFrom == 0 {
					return nil
				}

				bucketsDiscoveryPaginationRequest.From = resp.NextFrom
			}
		})
	}

	err := errGr.Wait()
	if err != nil {
		return fmt.Errorf("errGr.Wait() err: %w", err)
	}
	r.log().Infof(ctx, "discovery done since: %s", time.Since(t))

	return nil
}

// cronDiscovery is discovery_service_f analog with goroutines instead fibers
func (r *Router) cronDiscovery(ctx context.Context) {
	var iterationCount uint64

	for {
		select {
		case <-ctx.Done():
			r.metrics().CronDiscoveryEvent(false, 0, "ctx-cancel")
			r.log().Infof(ctx, "[DISCOVERY] cron discovery has been stopped after %d iterations", iterationCount)
			return
		case <-time.After(r.cfg.DiscoveryTimeout):
			iterationCount++
		}

		// Since the current for loop should not stop until ctx->Done() event fires,
		// we should be able to continue execution even a panic occures.
		// Therefore, we should wrap everyting into anonymous function that recovers after panic.
		// (Similar to pcall in lua/tarantool)
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					// Another one panic may happen due to log function below (e.g. bug in log().Errorf), in this case we have two options:
					// 1. recover again and log nothing: panic will be muted and lost
					// 2. don't try to recover, we hope that the second panic will be logged somehow by go runtime
					// So, we choose the second behavior
					r.log().Errorf(ctx, "[DISCOVERY] something unexpected has happened in cronDiscovery(%d): panic %v, stackstrace: %s",
						iterationCount, recovered, string(debug.Stack()))
				}
			}()

			r.log().Infof(ctx, "[DISCOVERY] started cron discovery iteration %d", iterationCount)

			tStartDiscovery := time.Now()

			if err := r.DiscoveryAllBuckets(ctx); err != nil {
				r.metrics().CronDiscoveryEvent(false, time.Since(tStartDiscovery), "discovery-error")
				r.log().Errorf(ctx, "[DISCOVERY] cant do cron discovery iteration %d with error: %s", iterationCount, err)
				return
			}

			r.log().Infof(ctx, "[DISCOVERY] finished cron discovery iteration %d", iterationCount)

			r.metrics().CronDiscoveryEvent(true, time.Since(tStartDiscovery), "ok")
		}()
	}
}

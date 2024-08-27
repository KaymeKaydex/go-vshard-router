package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	view := r.getConsistentView()

	rs := view.routeMap[bucketID].Load()
	if rs != nil {
		return rs, nil
	}

	// it`s ok if in the same time we have few active searches
	// mu per bucket is expansive
	r.log().Infof(ctx, "Discovering bucket %d", bucketID)

	idToReplicasetRef := r.getIDToReplicaset()

	wg := sync.WaitGroup{}
	wg.Add(len(idToReplicasetRef))

	type result struct {
		err error
		rs  *Replicaset
	}

	// This works only for go 1.19 or higher. To support older versions
	// we can use mutex + conditional compilation that checks go version.
	// Example for conditional compilation: https://www.youtube.com/watch?v=5eQBKqVlNQg
	var resultAtomic = atomic.Pointer[result]{}

	for rsID, rs := range idToReplicasetRef {
		go func(rs *Replicaset, rsID uuid.UUID) {
			defer wg.Done()
			if _, err := rs.BucketStat(ctx, bucketID); err == nil {
				// It's ok if several replicasets return ok to bucket_stat command for the same bucketID,
				// just pick any of them.
				var res result
				res.rs, res.err = r.BucketSet(bucketID, rsID)
				resultAtomic.Store(&res)
			}
		}(rs, rsID)
	}

	wg.Wait()

	res := resultAtomic.Load()
	resultRs, err := res.rs, res.err

	if err != nil || resultRs == nil {
		return nil, Errors[9] // NO_ROUTE_TO_BUCKET
	}
	/*
	   -- All replicasets were scanned, but a bucket was not
	   -- found anywhere, so most likely it does not exist. It
	   -- can be wrong, if rebalancing is in progress, and a
	   -- bucket was found to be RECEIVING on one replicaset, and
	   -- was not found on other replicasets (it was sent during
	   -- discovery).
	*/

	return resultRs, nil
}

// BucketResolve resolve bucket id to replicaset
func (r *Router) BucketResolve(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	if bucketID > r.cfg.TotalBucketCount {
		return nil, fmt.Errorf("bucket id is out of range: %d (total %d)", bucketID, r.cfg.TotalBucketCount)
	}

	view := r.getConsistentView()

	rs := view.routeMap[bucketID].Load()
	if rs != nil {
		return rs, nil
	}

	// Replicaset removed from cluster, perform discovery
	rs, err := r.BucketDiscovery(ctx, bucketID)
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// DiscoveryHandleBuckets arrange downloaded buckets to the route map so as they reference a given replicaset.
func (r *Router) DiscoveryHandleBuckets(ctx context.Context, rs *Replicaset, buckets []uint64) {
	view := r.getConsistentView()

	count := rs.bucketCount.Load()

	affected := make(map[*Replicaset]int)

	for _, bucketID := range buckets {
		oldRs := view.routeMap[bucketID].Swap(rs)

		if oldRs != rs {
			count++

			if oldRs != nil {
				if _, exists := affected[oldRs]; !exists {
					affected[oldRs] = int(oldRs.bucketCount.Load())
				}

				oldRs.bucketCount.Add(-1)
			} else {
				//                 router.known_bucket_count = router.known_bucket_count + 1
				view.knownBucketCount.Add(1)
			}
		}
	}

	if count != rs.bucketCount.Load() {
		r.log().Infof(ctx, "Updated %s buckets: was %d, became %d", rs.info.Name, rs.bucketCount.Load(), count)
	}

	rs.bucketCount.Store(count)

	for rs, oldBucketCount := range affected {
		r.log().Infof(ctx, "Affected buckets of %s: was %d, became %d", rs.info.Name, oldBucketCount, rs.bucketCount.Load())
	}
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

// startCronDiscovery is discovery_service_f analog with goroutines instead fibers
func (r *Router) startCronDiscovery(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			r.metrics().CronDiscoveryEvent(false, 0, "ctx-cancel")

			return ctx.Err()
		case <-time.After(r.cfg.DiscoveryTimeout):
			r.log().Debugf(ctx, "started new cron discovery")

			tStartDiscovery := time.Now()

			defer func() {
				r.log().Infof(ctx, "discovery done since %s", time.Since(tStartDiscovery))
			}()

			err := r.DiscoveryAllBuckets(ctx)
			if err != nil {
				r.metrics().CronDiscoveryEvent(false, time.Since(tStartDiscovery), "discovery-error")

				r.log().Errorf(ctx, "cant do cron discovery with error: %s", err)
			}

			r.metrics().CronDiscoveryEvent(true, time.Since(tStartDiscovery), "ok")
		}
	}
}

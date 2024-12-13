package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

type ReplicasetInfo struct {
	Name             string
	UUID             uuid.UUID
	Weight           float64
	PinnedCount      uint64
	IgnoreDisbalance bool
}

func (rsi ReplicasetInfo) String() string {
	return fmt.Sprintf("{name: %s, uuid: %s}", rsi.Name, rsi.UUID)
}

type ReplicasetCallOpts struct {
	PoolMode pool.Mode
	Timeout  time.Duration
}

type Replicaset struct {
	conn              pool.Pooler
	info              ReplicasetInfo
	EtalonBucketCount uint64
}

func (rs *Replicaset) String() string {
	return rs.info.String()
}

func (rs *Replicaset) BucketStat(ctx context.Context, bucketID uint64) (BucketStatInfo, error) {
	future := rs.bucketStatAsync(ctx, bucketID)

	return bucketStatWait(future)
}

func (rs *Replicaset) bucketStatAsync(ctx context.Context, bucketID uint64) *tarantool.Future {
	const bucketStatFnc = "vshard.storage.bucket_stat"

	return rs.CallAsync(ctx, ReplicasetCallOpts{PoolMode: pool.RO}, bucketStatFnc, []interface{}{bucketID})
}

func bucketStatWait(future *tarantool.Future) (BucketStatInfo, error) {
	var bsInfo BucketStatInfo

	respData, err := future.Get()
	if err != nil {
		return bsInfo, err
	}

	if len(respData) == 0 {
		return bsInfo, fmt.Errorf("protocol violation bucketStatWait: empty response")
	}

	if respData[0] == nil {
		if len(respData) != 2 {
			return bsInfo, fmt.Errorf("protocol violation bucketStatWait: invalid response length %d when respData[0] is nil", len(respData))
		}

		var bsError bucketStatError
		err = mapstructure.Decode(respData[1], &bsError)
		if err != nil {
			// We could not decode respData[1] as bsError, so return respData[1] as is, add info why we could not decode.
			return bsInfo, fmt.Errorf("bucketStatWait error: %v (can't decode into bsError: %v)", respData[1], err)
		}

		return bsInfo, bsError
	}

	// A problem with key-code 1
	// todo: fix after https://github.com/tarantool/go-tarantool/issues/368
	err = mapstructure.Decode(respData[0], &bsInfo)
	if err != nil {
		return bsInfo, fmt.Errorf("can't decode bsInfo: %w", err)
	}

	return bsInfo, nil
}

// ReplicaCall perform function on remote storage
// link https://github.com/tarantool/vshard/blob/master/vshard/replicaset.lua#L661
// This method is deprecated, because looks like it has a little bit broken interface
func (rs *Replicaset) ReplicaCall(
	ctx context.Context,
	opts ReplicasetCallOpts,
	fnc string,
	args interface{},
) (interface{}, StorageResultTypedFunc, error) {
	if opts.Timeout == 0 {
		opts.Timeout = CallTimeoutMin
	}

	future := rs.CallAsync(ctx, opts, fnc, args)

	respData, err := future.Get()
	if err != nil {
		return nil, nil, fmt.Errorf("got error on future.Get(): %w", err)
	}

	if len(respData) == 0 {
		// Since this method returns the first element of respData by contract, we can't return anything is this case (broken interface)
		return nil, nil, fmt.Errorf("%s response data is empty", fnc)
	}

	return respData[0], func(result interface{}) error {
		return future.GetTyped(&[]interface{}{&result})
	}, nil
}

// CallAsync sends async request to remote storage
func (rs *Replicaset) CallAsync(ctx context.Context, opts ReplicasetCallOpts, fnc string, args interface{}) *tarantool.Future {
	if opts.Timeout > 0 {
		// Don't set any timeout by default, parent context timeout would be inherited in this case.
		// Don't call cancel in defer, because this we send request asynchronously,
		// and wait for result outside from this function.
		// suppress linter warning: lostcancel: the cancel function returned by context.WithTimeout should be called, not discarded, to avoid a context leak (govet)
		//nolint:govet
		ctx, _ = context.WithTimeout(ctx, opts.Timeout)
	}

	req := tarantool.NewCallRequest(fnc).
		Context(ctx).
		Args(args)

	return rs.conn.Do(req, opts.PoolMode)
}

func (rs *Replicaset) bucketsDiscoveryAsync(ctx context.Context, from uint64) *tarantool.Future {
	const bucketsDiscoveryFnc = "vshard.storage.buckets_discovery"

	var bucketsDiscoveryPaginationRequest = struct {
		From uint64 `msgpack:"from"`
	}{From: from}

	return rs.CallAsync(ctx, ReplicasetCallOpts{PoolMode: pool.PreferRO}, bucketsDiscoveryFnc,
		[]interface{}{bucketsDiscoveryPaginationRequest})
}

type bucketsDiscoveryResp struct {
	Buckets  []uint64 `msgpack:"buckets"`
	NextFrom uint64   `msgpack:"next_from"`
}

func bucketsDiscoveryWait(future *tarantool.Future) (bucketsDiscoveryResp, error) {
	// We intentionally don't support old vshard storages that mentioned here:
	// https://github.com/tarantool/vshard/blob/8d299bfecff8bc656056658350ad48c829f9ad3f/vshard/router/init.lua#L343
	var resp bucketsDiscoveryResp

	err := future.GetTyped(&[]interface{}{&resp})
	if err != nil {
		return resp, fmt.Errorf("future.GetTyped() failed: %v", err)
	}

	return resp, nil
}

func (rs *Replicaset) bucketsDiscovery(ctx context.Context, from uint64) (bucketsDiscoveryResp, error) {
	future := rs.bucketsDiscoveryAsync(ctx, from)

	return bucketsDiscoveryWait(future)
}

// CalculateEtalonBalance computes the ideal bucket count for each replicaset.
// This iterative algorithm seeks the optimal balance within a cluster by
// calculating the ideal bucket count for each replicaset at every step.
// If the ideal count cannot be achieved due to pinned buckets, the algorithm
// makes a best effort to approximate balance by ignoring the replicaset with
// pinned buckets and its associated pinned count. After each iteration, a new
// balance is recalculated. However, this can lead to scenarios where the
// conditions are still unmet; ignoring pinned buckets in overloaded
// replicasets can reduce the ideal bucket count in others, potentially
// causing new values to fall below their pinned count.
//
// At each iteration, the algorithm either concludes or disregards at least
// one new overloaded replicaset. Therefore, its time complexity is O(N^2),
// where N is the number of replicasets.
// based on  https://github.com/tarantool/vshard/blob/master/vshard/replicaset.lua#L1358
func CalculateEtalonBalance(replicasets []Replicaset, bucketCount uint64) error {
	isBalanceFound := false
	weightSum := 0.0
	stepCount := 0
	replicasetCount := len(replicasets)

	// Calculate total weight
	for _, replicaset := range replicasets {
		weightSum += replicaset.info.Weight
	}

	// Balance calculation loop
	for !isBalanceFound {
		stepCount++
		if weightSum <= 0 {
			return fmt.Errorf("weightSum should be greater than 0")
		}

		bucketPerWeight := float64(bucketCount) / weightSum
		bucketsCalculated := uint64(0)

		// Calculate etalon bucket count for each replicaset
		for i := range replicasets {
			if !replicasets[i].info.IgnoreDisbalance {
				replicasets[i].EtalonBucketCount = uint64(math.Ceil(replicasets[i].info.Weight * bucketPerWeight))
				bucketsCalculated += replicasets[i].EtalonBucketCount
			}
		}

		bucketsRest := bucketsCalculated - bucketCount
		isBalanceFound = true

		// Spread disbalance and check for pinned buckets
		for i := range replicasets {
			if !replicasets[i].info.IgnoreDisbalance {
				if bucketsRest > 0 {
					n := replicasets[i].info.Weight * bucketPerWeight
					ceil := math.Ceil(n)
					floor := math.Floor(n)
					if replicasets[i].EtalonBucketCount > 0 && ceil != floor {
						replicasets[i].EtalonBucketCount--
						bucketsRest--
					}
				}

				// Handle pinned buckets
				pinned := replicasets[i].info.PinnedCount
				if pinned > 0 && replicasets[i].EtalonBucketCount < pinned {
					isBalanceFound = false
					bucketCount -= pinned
					replicasets[i].EtalonBucketCount = pinned
					replicasets[i].info.IgnoreDisbalance = true
					weightSum -= replicasets[i].info.Weight
				}
			}
		}

		if bucketsRest != 0 {
			return fmt.Errorf("bucketsRest should be 0")
		}

		// Safety check to prevent infinite loops
		if stepCount > replicasetCount {
			return fmt.Errorf("[PANIC]: the rebalancer is broken")
		}
	}

	return nil
}

func (rs *Replicaset) BucketsCount(ctx context.Context) (uint64, error) {
	const bucketCountFnc = "vshard.storage.buckets_count"

	var bucketCount uint64

	fut := rs.CallAsync(ctx, ReplicasetCallOpts{PoolMode: pool.ANY}, bucketCountFnc, nil)
	err := fut.GetTyped(&[]interface{}{&bucketCount})

	return bucketCount, err
}

func (rs *Replicaset) BucketForceCreate(ctx context.Context, firstBucketID, count uint64) error {
	const bucketForceCreateFnc = "vshard.storage.bucket_force_create"

	fut := rs.CallAsync(ctx, ReplicasetCallOpts{PoolMode: pool.RW}, bucketForceCreateFnc, []interface{}{firstBucketID, count})
	_, err := fut.GetResponse()

	return err
}

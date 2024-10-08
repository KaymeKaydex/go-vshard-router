package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

type ReplicasetInfo struct {
	Name string
	UUID uuid.UUID
}

func (rsi ReplicasetInfo) String() string {
	return fmt.Sprintf("{name: %s, uuid: %s}", rsi.Name, rsi.UUID)
}

type ReplicasetCallOpts struct {
	PoolMode pool.Mode
	Timeout  time.Duration
}

type Replicaset struct {
	conn pool.Pooler
	info ReplicasetInfo
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

	req := tarantool.NewCallRequest(bucketStatFnc).
		Args([]interface{}{bucketID}).
		Context(ctx)

	future := rs.conn.Do(req, pool.RO)

	return future
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
	timeout := CallTimeoutMin

	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req := tarantool.NewCallRequest(fnc).
		Context(ctx).
		Args(args)

	future := rs.conn.Do(req, opts.PoolMode)

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

// Call sends async request to remote storage
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

	req := tarantool.NewCallRequest(bucketsDiscoveryFnc).
		Context(ctx).
		Args([]interface{}{&bucketsDiscoveryPaginationRequest})

	future := rs.conn.Do(req, pool.PreferRO)

	return future
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

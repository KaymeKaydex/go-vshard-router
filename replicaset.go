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
	const bucketStatFnc = "vshard.storage.bucket_stat"

	var bsInfo BucketStatInfo

	req := tarantool.NewCallRequest(bucketStatFnc).
		Args([]interface{}{bucketID}).
		Context(ctx)

	future := rs.conn.Do(req, pool.RO)
	respData, err := future.Get()
	if err != nil {
		return bsInfo, err
	}

	if len(respData) < 1 {
		return bsInfo, fmt.Errorf("respData len is 0 for %s; unsupported or broken proto", bucketStatFnc)
	}

	if respData[0] == nil {

		if len(respData) < 2 {
			return bsInfo, fmt.Errorf("respData len < 2 when respData[0] is nil for %s", bucketStatFnc)
		}

		var tmp interface{} // todo: fix non-panic crutch
		bsError := &BucketStatError{}

		err := future.GetTyped(&[]interface{}{tmp, bsError})
		if err != nil {
			return bsInfo, err
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

	timeStart := time.Now()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req := tarantool.NewCallRequest(fnc).
		Context(ctx).
		Args(args)

	var (
		respData []interface{}
		err      error
	)

	for {
		if since := time.Since(timeStart); since > timeout {
			return nil, nil, err
		}

		future := rs.conn.Do(req, opts.PoolMode)

		respData, err = future.Get()
		if err != nil {
			continue
		}

		if len(respData) == 0 {
			// Since this method returns the first element of respData by contract, we can't return anything is this case (broken interface)
			err = fmt.Errorf("response data is empty")
			continue
		}

		return respData[0], func(result interface{}) error {
			return future.GetTyped(&[]interface{}{&result})
		}, nil
	}
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

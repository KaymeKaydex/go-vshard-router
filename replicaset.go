package vshard_router

import (
	"context"
	"fmt"
	"sync/atomic"
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

type Pool interface {
	pool.Pooler
	Add(ctx context.Context, instance pool.Instance) error
	Remove(name string) error
	CloseGraceful() []error
}

type Replicaset struct {
	conn Pool
	info ReplicasetInfo

	bucketCount atomic.Int32
}

func (rs *Replicaset) String() string {
	return rs.info.String()
}

func (rs *Replicaset) BucketStat(ctx context.Context, bucketID uint64) (BucketStatInfo, error) {
	bsInfo := &BucketStatInfo{}
	bsError := &BucketStatError{}

	req := tarantool.NewCallRequest("vshard.storage.bucket_stat").
		Args([]interface{}{bucketID}).
		Context(ctx)

	future := rs.conn.Do(req, pool.RO)
	respData, err := future.Get()
	if err != nil {
		return BucketStatInfo{}, err
	}

	var tmp interface{} // todo: fix non-panic crutch

	if respData[0] == nil {
		err := future.GetTyped(&[]interface{}{tmp, bsError})
		if err != nil {
			return BucketStatInfo{}, err
		}
	} else {
		// fucking key-code 1
		// todo: fix after https://github.com/tarantool/go-tarantool/issues/368
		err := mapstructure.Decode(respData[0], bsInfo)
		if err != nil {
			return BucketStatInfo{}, err
		}
	}

	return *bsInfo, bsError
}

// ReplicaCall perform function on remote storage
// link https://github.com/tarantool/vshard/blob/master/vshard/replicaset.lua#L661
func (rs *Replicaset) ReplicaCall(
	ctx context.Context,
	opts ReplicasetCallOpts,
	fnc string,
	args interface{},
) (interface{}, StorageResultTypedFunc, error) {
	if opts.Timeout == 0 {
		opts.Timeout = CallTimeoutMin
	}

	timeout := opts.Timeout
	timeStart := time.Now()

	req := tarantool.NewCallRequest(fnc)
	req = req.Context(ctx)
	req = req.Args(args)

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

		if len(respData) != 2 {
			err = fmt.Errorf("invalid length of response data: must be = 2, current: %d", len(respData))
			continue
		}

		if respData[1] != nil {
			assertErr := &StorageCallAssertError{}

			err = mapstructure.Decode(respData[1], assertErr)
			if err != nil {
				continue
			}

			err = assertErr
			continue
		}

		return respData[0], func(result interface{}) error {
			return future.GetTyped(&[]interface{}{&result})
		}, nil
	}
}

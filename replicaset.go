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

type Replicaset struct {
	conn *pool.ConnectionPool
	info ReplicasetInfo

	bucketCount atomic.Int32
}

type ReplicasetCallOpts struct {
	PoolMode pool.Mode
	Timeout  time.Duration
}

// ReplicasetCallImpl perform function on remote storage
// link https://github.com/tarantool/vshard/blob/master/vshard/replicaset.lua#L661
func (rs *Replicaset) ReplicasetCallImpl(
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

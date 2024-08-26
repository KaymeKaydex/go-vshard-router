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

// --------------------------------------------------------------------------------
// -- API
// --------------------------------------------------------------------------------

type VshardMode string

const (
	ReadMode  VshardMode = "read"
	WriteMode VshardMode = "write"
)

func (c VshardMode) String() string {
	return string(c)
}

type StorageCallAssertError struct {
	Code     int         `msgpack:"code"`
	BaseType string      `msgpack:"base_type"`
	Type     string      `msgpack:"type"`
	Message  string      `msgpack:"message"`
	Trace    interface{} `msgpack:"trace"`
}

func (s StorageCallAssertError) Error() string {
	return fmt.Sprintf("vshard.storage.call assert error code: %d, type:%s, message: %s", s.Code, s.Type, s.Message)
}

type StorageCallVShardError struct {
	BucketID       uint64  `msgpack:"bucket_id" mapstructure:"bucket_id"`
	Reason         string  `msgpack:"reason"`
	Code           int     `msgpack:"code"`
	Type           string  `msgpack:"type"`
	Message        string  `msgpack:"message"`
	Name           string  `msgpack:"name"`
	MasterUUID     *string `msgpack:"master_uuid" mapstructure:"master_uuid"`         // mapstructure cant decode to source uuid type
	ReplicasetUUID *string `msgpack:"replicaset_uuid" mapstructure:"replicaset_uuid"` // mapstructure cant decode to source uuid type
}

func (s StorageCallVShardError) Error() string {
	return fmt.Sprintf("vshard.storage.call bucket error bucket_id: %d, reason: %s, name: %s", s.BucketID, s.Reason, s.Name)
}

type StorageResultTypedFunc = func(result interface{}) error

type CallOpts struct {
	VshardMode VshardMode // vshard mode in call
	PoolMode   pool.Mode
	Timeout    time.Duration
}

// revive warns us: time-naming: var CallTimeoutMin is of type time.Duration; don't use unit-specific suffix "Min".
// But the original lua vshard implementation uses this naming, so we use it too.
//
//nolint:revive
const CallTimeoutMin = time.Second / 2

// RouterCallImpl Perform shard operation function will restart operation
// after wrong bucket response until timeout is reached
func (r *Router) RouterCallImpl(ctx context.Context,
	bucketID uint64,
	opts CallOpts,
	fnc string,
	args interface{}) (interface{}, StorageResultTypedFunc, error) {

	if bucketID > r.cfg.TotalBucketCount {
		return nil, nil, fmt.Errorf("bucket id is out of range: %d (total %d)", bucketID, r.cfg.TotalBucketCount)
	}

	if opts.Timeout == 0 {
		opts.Timeout = CallTimeoutMin
	}

	timeout := opts.Timeout
	timeStart := time.Now()

	req := tarantool.NewCallRequest("vshard.storage.call")
	req = req.Context(ctx)
	req = req.Args([]interface{}{
		bucketID,
		opts.VshardMode.String(),
		fnc,
		args,
	})

	var err error

	for {
		if since := time.Since(timeStart); since > timeout {
			r.metrics().RequestDuration(since, false, false)

			r.log().Debug(ctx, fmt.Sprintf("return result on timeout; since %s of timeout %s", since, timeout))
			if err == nil {
				err = fmt.Errorf("cant get call cause call impl timeout")
			}

			return nil, nil, err
		}

		var rs *Replicaset

		rs, err = r.BucketResolve(ctx, bucketID)
		if err != nil {
			r.log().Debug(ctx, fmt.Sprintf("cant resolve bucket %d with error: %s", bucketID, err.Error()))

			r.metrics().RetryOnCall("bucket_resolve_error")
			continue
		}

		r.log().Info(ctx, fmt.Sprintf("try call %s on replicaset %s for bucket %d", fnc, rs.info.Name, bucketID))

		future := rs.conn.Do(req, opts.PoolMode)

		var respData []interface{}
		respData, err = future.Get()
		if err != nil {
			r.log().Error(ctx, fmt.Sprintf("got future error: %s", err))
			r.metrics().RetryOnCall("future_get_error")

			continue
		}

		r.log().Debug(ctx, fmt.Sprintf("got call result response data %s", respData))

		if len(respData) < 1 {
			// vshard.storage.call(func) returns up to two values:
			// - true/false
			// - func result, omitted if func does not return anything
			err = fmt.Errorf("invalid length of response data: must be >= 1, current: %d", len(respData))

			r.log().Error(ctx, err.Error())

			r.metrics().RetryOnCall("resp_data_error")
			continue
		}

		if respData[0] == nil {
			vshardErr := &StorageCallVShardError{}

			if len(respData) < 2 {
				err = fmt.Errorf("unexpected response length when respData[0] == nil: %d", len(respData))
			} else {
				err = mapstructure.Decode(respData[1], vshardErr)
			}

			if err != nil {
				r.metrics().RetryOnCall("internal_error")

				err = fmt.Errorf("cant decode vhsard err by trarantool with err: %s; continue try", err)

				r.log().Error(ctx, err.Error())
				continue
			}

			err = vshardErr

			r.log().Error(ctx, fmt.Sprintf("got vshard storage call error: %s", err))

			if vshardErr.Name == "WRONG_BUCKET" ||
				vshardErr.Name == "BUCKET_IS_LOCKED" ||
				vshardErr.Name == "TRANSFER_IS_IN_PROGRESS" {
				r.BucketReset(bucketID)
				r.metrics().RetryOnCall("bucket_migrate")

				continue
			}

			continue
		}

		isVShardRespOk := false
		err = future.GetTyped(&[]interface{}{&isVShardRespOk})
		if err != nil {
			r.log().Debug(ctx, fmt.Sprintf("cant get typed with err: %s", err))

			continue
		}

		if !isVShardRespOk { // error
			errorResp := &StorageCallAssertError{}

			// Since we got respData[0] == false, it means that assert has happened
			// while executing user-defined function on vshard storage.
			// In this case, vshard storage must return a pair: false, error.
			if len(respData) < 2 {
				err = fmt.Errorf("protocol violation: unexpected response length when respData[0] == false: %d", len(respData))
			} else {
				err = future.GetTyped(&[]interface{}{&isVShardRespOk, errorResp})
			}

			if err != nil {
				// Either protocol has been violated or decoding has failed.
				err = fmt.Errorf("cant get typed vshard err with err: %s", err)
			} else {
				// StorageCallAssertError successfully has been decoded.
				err = errorResp
			}

			continue
		}

		r.metrics().RequestDuration(time.Since(timeStart), true, false)

		return respData[1:], func(result interface{}) error {
			if len(respData) < 2 {
				return nil
			}

			var stub interface{}

			return future.GetTyped(&[]interface{}{&stub, result})
		}, nil
	}
}

// RouterMapCallRWImpl perform call function on all masters in the cluster
// with a guarantee that in case of success it was executed with all
// buckets being accessible for reads and writes.
func (r *Router) RouterMapCallRWImpl(
	ctx context.Context,
	fnc string,
	args interface{},
	opts CallOpts,
) (map[uuid.UUID]interface{}, error) {
	const vshardStorageServiceCall = "vshard.storage._call"

	timeout := CallTimeoutMin
	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}

	timeStart := time.Now()
	refID := r.refID.Add(1)

	idToReplicasetRef := r.getIDToReplicaset()

	defer func() {
		// call function "storage_unref" if map_callrw is failed or successed
		storageUnrefReq := tarantool.NewCallRequest(vshardStorageServiceCall).
			Args([]interface{}{"storage_unref", refID})

		for _, rs := range idToReplicasetRef {
			future := rs.conn.Do(storageUnrefReq, pool.RW)
			future.SetError(nil)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// ref stage

	storageRefReq := tarantool.NewCallRequest(vshardStorageServiceCall).
		Context(ctx).
		Args([]interface{}{"storage_ref", refID, timeout})

	type replicasetFuture struct {
		uuid   uuid.UUID
		future *tarantool.Future
	}

	var rsFutures = make([]replicasetFuture, 0, len(idToReplicasetRef))

	// ref stage: send concurrent ref requests
	for uuid, rs := range idToReplicasetRef {
		rsFutures = append(rsFutures, replicasetFuture{
			uuid:   uuid,
			future: rs.conn.Do(storageRefReq, pool.RW),
		})
	}

	// ref stage: get their responses
	var totalBucketCount uint64
	for _, rsFuture := range rsFutures {
		respData, err := rsFuture.future.Get()
		if err != nil {
			return nil, fmt.Errorf("rs {%s} storage_ref err: %v", rsFuture.uuid, err)
		}

		if respData[0] == nil {
			vshardErr := &StorageCallAssertError{}

			err = mapstructure.Decode(respData[1], vshardErr)
			if err != nil {
				return nil, err
			}

			return nil, vshardErr
		}

		var bucketCount uint64
		err = rsFuture.future.GetTyped(&[]interface{}{&bucketCount})
		if err != nil {
			return nil, err
		}

		totalBucketCount += bucketCount
	}

	if totalBucketCount != r.cfg.TotalBucketCount {
		return nil, fmt.Errorf("total bucket count got %d, expected %d", totalBucketCount, r.cfg.TotalBucketCount)
	}

	// map stage

	storageMapReq := tarantool.NewCallRequest(vshardStorageServiceCall).
		Context(ctx).
		Args([]interface{}{"storage_map", refID, fnc, args})

	// reuse the same slice again
	rsFutures = rsFutures[0:0]

	// map stage: send concurrent map requests
	for uuid, rs := range idToReplicasetRef {
		rsFutures = append(rsFutures, replicasetFuture{
			uuid:   uuid,
			future: rs.conn.Do(storageMapReq, pool.RW),
		})
	}

	// map stage: get their responses
	idToResult := make(map[uuid.UUID]interface{})
	for _, rsFuture := range rsFutures {
		respData, err := rsFuture.future.Get()
		if err != nil {
			return nil, fmt.Errorf("rs {%s} storage_map err: %v", rsFuture.uuid, err)
		}

		if len(respData) != 2 {
			return nil, fmt.Errorf("invalid length of response data: must be = 2, current: %d", len(respData))
		}

		if respData[0] == nil {
			vshardErr := &StorageCallAssertError{}

			err = mapstructure.Decode(respData[1], vshardErr)
			if err != nil {
				return nil, err
			}

			return nil, vshardErr
		}

		var isVShardRespOk bool
		err = rsFuture.future.GetTyped(&[]interface{}{&isVShardRespOk})
		if err != nil {
			return nil, err
		}

		if !isVShardRespOk { // error
			errorResp := &StorageCallAssertError{}

			err = rsFuture.future.GetTyped(&[]interface{}{&isVShardRespOk, errorResp})
			if err != nil {
				return nil, fmt.Errorf("cant get typed vshard err with err: %v", err)
			}

			return nil, errorResp
		}

		idToResult[rsFuture.uuid] = respData[1]
	}

	r.metrics().RequestDuration(time.Since(timeStart), true, true)

	return idToResult, nil
}

// RouterRoute get replicaset object by bucket identifier.
// alias to BucketResolve
func (r *Router) RouterRoute(ctx context.Context, bucketID uint64) (*Replicaset, error) {
	return r.BucketResolve(ctx, bucketID)
}

// RouterRouteAll return map of all replicasets.
func (r *Router) RouterRouteAll() map[uuid.UUID]*Replicaset {
	idToReplicasetRef := r.getIDToReplicaset()

	// Do not expose the original map to prevent unauthorized modification.
	idToReplicasetCopy := make(map[uuid.UUID]*Replicaset)

	for k, v := range idToReplicasetRef {
		idToReplicasetCopy[k] = v
	}

	return idToReplicasetCopy
}

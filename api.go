package vshard_router //nolint:revive

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
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

type VShardResponse struct {
	assertError *assertError  // not nil if there is assert error
	vshardError *vshardError  // not nil if there is vshard response
	data        []interface{} // raw response data
}

func (s *VShardResponse) DecodeMsgpack(d *msgpack.Decoder) error {
	// get array len for protocol violation check
	arrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrayLen == 0 {
		// vshard.storage.call(func) returns up to two values:
		// - true/false/nil
		// - func result, omitted if func does not return anything
		return fmt.Errorf("invalid array length: %d; protocol violation", arrayLen)
	}

	// we need peek code to make our check faster than decode interface
	// later we will check if code nil or bool
	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	// this is storage error
	if code == msgpcode.Nil {
		err = d.DecodeNil()
		if err != nil {
			return err
		}

		ve := &vshardError{}
		err = d.Decode(ve)
		if err != nil {
			return fmt.Errorf("failed to decode storage assert error: %w", err)
		}

		s.vshardError = ve
		return nil
	}

	assertBoolOk, err := d.DecodeBool()
	if err != nil {
		return err
	}

	// that means we have no assert errors and response ok
	if assertBoolOk {
		data := make([]interface{}, 0, arrayLen-1)
		for i := 1; i < arrayLen; i++ {
			var face interface{}
			face, err = d.DecodeInterface()

			data = append(data, face)
		}

		s.data = data

		return err
	} else {
		ae := &assertError{}
		err = d.Decode(ae)
		if err != nil {
			return fmt.Errorf("failed to decode storage assert error: %w", err)
		}

		s.assertError = ae
		return nil
	}
}

type assertError struct {
	Code     int                      `msgpack:"code"`
	BaseType string                   `msgpack:"base_type"`
	Type     string                   `msgpack:"type"`
	Message  string                   `msgpack:"message"`
	Trace    []map[string]interface{} `msgpack:"trace"`
}

func (s assertError) Error() string {
	// Just print struct as is, use hack with alias type to avoid recursion:
	// %v attempts to call Error() method for s, which is recursion.
	// This alias doesn't have method Error().
	type alias assertError
	return fmt.Sprintf("%+v", alias(s))
}

type vshardError struct {
	BucketID       uint64  `msgpack:"bucket_id" mapstructure:"bucket_id"`
	Reason         string  `msgpack:"reason"`
	Code           int     `msgpack:"code"`
	Type           string  `msgpack:"type"`
	Message        string  `msgpack:"message"`
	Name           string  `msgpack:"name"`
	MasterUUID     *string `msgpack:"master_uuid" mapstructure:"master_uuid"`         // mapstructure cant decode to source uuid type
	ReplicasetUUID *string `msgpack:"replicaset_uuid" mapstructure:"replicaset_uuid"` // mapstructure cant decode to source uuid type
}

func (s vshardError) Error() string {
	// Just print struct as is, use hack with alias type to avoid recursion:
	// %v attempts to call Error() method for s, which is recursion.
	// This alias doesn't have method Error().
	type alias vshardError
	return fmt.Sprintf("%+v", alias(s))
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

	const vshardStorageClientCall = "vshard.storage.call"

	if bucketID < 1 || r.cfg.TotalBucketCount < bucketID {
		return nil, nil, fmt.Errorf("bucket id is out of range: %d (total %d)", bucketID, r.cfg.TotalBucketCount)
	}

	if opts.Timeout == 0 {
		opts.Timeout = CallTimeoutMin
	}

	timeout := opts.Timeout
	timeStart := time.Now()

	req := tarantool.NewCallRequest(vshardStorageClientCall)
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

			r.log().Debugf(ctx, "return result on timeout; since %s of timeout %s", since, timeout)
			if err == nil {
				err = fmt.Errorf("cant get call cause call impl timeout")
			}

			return nil, nil, err
		}

		var rs *Replicaset

		rs, err = r.BucketResolve(ctx, bucketID)
		if err != nil {
			r.metrics().RetryOnCall("bucket_resolve_error")

			// this error will be returned to a caller in case of timeout
			err = fmt.Errorf("cant resolve bucket %d: %w", bucketID, err)

			// TODO: lua vshard router just yields here and retires, no pause is applied.
			// https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L713
			// So we also retry here. But I guess we should add some pause here.
			continue
		}

		r.log().Infof(ctx, "try call %s on replicaset %s for bucket %d", fnc, rs.info.Name, bucketID)

		future := rs.conn.Do(req, opts.PoolMode)

		resp := &VShardResponse{}
		err = future.GetTyped(resp)
		if err != nil {
			return nil, nil, fmt.Errorf("got error on future.Get(): %w", err)
		}

		r.log().Debugf(ctx, "got call result response data %v", resp.data)

		if resp.vshardError != nil {
			vshardErr := resp.vshardError

			switch vshardErr.Name {
			case "WRONG_BUCKET", "BUCKET_IS_LOCKED":
				r.BucketReset(bucketID)

				// TODO we should inspect here err.destination like lua vshard router does,
				// but we don't support vshard error fully yet:
				// https://github.com/KaymeKaydex/go-vshard-router/issues/94
				// So we just retry here as a temporary solution.
				r.metrics().RetryOnCall("bucket_migrate")

				r.log().Debugf(ctx, "retrying fnc '%s' cause got vshard error: %v", fnc, vshardErr)

				// this vshardError will be returned to a caller in case of timeout
				err = vshardErr
				continue
			case "TRANSFER_IS_IN_PROGRESS":
				// Since lua vshard router doesn't retry here, we don't retry too.
				// There is a comment why lua vshard router doesn't retry:
				// https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L697
				r.BucketReset(bucketID)
				return nil, nil, vshardErr
			case "NON_MASTER":
				// We don't know how to handle this case yet, so just return it for now.
				// Here is issue for it: https://github.com/KaymeKaydex/go-vshard-router/issues/88
				return nil, nil, vshardErr
			default:
				return nil, nil, vshardErr
			}
		}

		if resp.assertError != nil {
			return nil, nil, fmt.Errorf("%s: %s failed: %+v", vshardStorageClientCall, fnc, resp.assertError)
		}

		r.metrics().RequestDuration(time.Since(timeStart), true, false)

		return resp.data, func(result interface{}) error {
			if len(resp.data) == 0 {
				return nil
			}

			var stub bool

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
			future.SetError(nil) // TODO: does it cancel the request above or not?
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
	// proto for 'storage_ref' method:
	// https://github.com/tarantool/vshard/blob/dfa2cc8a2aff221d5f421298851a9a229b2e0434/vshard/storage/init.lua#L3137
	for _, rsFuture := range rsFutures {
		respData, err := rsFuture.future.Get()
		if err != nil {
			return nil, fmt.Errorf("rs {%s} storage_ref err: %v", rsFuture.uuid, err)
		}

		if len(respData) < 1 {
			return nil, fmt.Errorf("protocol violation: storage_ref: expected len(respData) 1 or 2, got: %d", len(respData))
		}

		if respData[0] == nil {
			if len(respData) != 2 {
				return nil, fmt.Errorf("protocol vioaltion: storage_ref: expected len(respData) = 2 when respData[0] == nil, got %d", len((respData)))
			}

			// The possible variations of error in respData[1] are fully unknown yet for us, this question requires research.
			// So we do not convert respData[1] to some known error format, because we don't use it anyway.
			return nil, fmt.Errorf("storage_ref failed on %v: %v", rsFuture.uuid, respData[1])
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
	// proto for 'storage_map' method:
	// https://github.com/tarantool/vshard/blob/8d299bfecff8bc656056658350ad48c829f9ad3f/vshard/storage/init.lua#L3158
	for _, rsFuture := range rsFutures {
		resp := &VShardResponse{}

		err := rsFuture.future.GetTyped(resp)
		if err != nil {
			return nil, fmt.Errorf("rs {%s} storage_map err: %v", rsFuture.uuid, err)
		}

		if resp.vshardError != nil {
			return nil, fmt.Errorf("storage_map failed on %v: %+v", rsFuture.uuid, resp.vshardError)
		}

		if resp.assertError != nil {
			return nil, fmt.Errorf("protocol violation: isVShardRespOk = false from storage_map: replicaset %v", rsFuture.uuid)
		}

		if resp.data == nil {
			idToResult[rsFuture.uuid] = nil
		} else {
			idToResult[rsFuture.uuid] = resp.data
		}
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

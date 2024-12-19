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

	// callTimeoutDefault is a default timeout when no timeout is provided
	callTimeoutDefault = 500 * time.Millisecond
)

func (c VshardMode) String() string {
	return string(c)
}

type vshardStorageCallResponseProto struct {
	AssertError *assertError            // not nil if there is assert error
	VshardError *StorageCallVShardError // not nil if there is vshard response
	CallResp    VshardRouterCallResp
}

func (r *vshardStorageCallResponseProto) DecodeMsgpack(d *msgpack.Decoder) error {
	/* vshard.storage.call(func) response has the next 4 possbile formats:
	See: https://github.com/tarantool/vshard/blob/dfa2cc8a2aff221d5f421298851a9a229b2e0434/vshard/storage/init.lua#L3130
	1. vshard error has occurred:
		array[nil, vshard_error]
	2. User method has finished with some error:
		array[false, assert error]
	3. User mehod has finished successfully
		a) but has not returned anything
			array[true]
		b) has returned 1 element
			array[true, elem1]
		c) has returned 2 element
			array[true, elem1, elem2]
		d) has returned 3 element
			array[true, elem1, elem2, elem3]
	*/

	// Ensure it is an array and get array len for protocol violation check
	respArrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if respArrayLen == 0 {
		return fmt.Errorf("protocol violation: invalid array length: %d", respArrayLen)
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

		if respArrayLen != 2 {
			return fmt.Errorf("protocol violation: length is %d on vshard error case", respArrayLen)
		}

		var vshardError StorageCallVShardError

		err = d.Decode(&vshardError)
		if err != nil {
			return fmt.Errorf("failed to decode storage vshard error: %w", err)
		}

		r.VshardError = &vshardError

		return nil
	}

	isVShardRespOk, err := d.DecodeBool()
	if err != nil {
		return err
	}

	if !isVShardRespOk {
		// that means we have an assert errors and response is not ok
		if respArrayLen != 2 {
			return fmt.Errorf("protocol violation: length is %d on assert error case", respArrayLen)
		}

		var assertError assertError
		err = d.Decode(&assertError)
		if err != nil {
			return fmt.Errorf("failed to decode storage assert error: %w", err)
		}

		r.AssertError = &assertError

		return nil
	}

	// isVShardRespOk is true
	r.CallResp.rawMessages = make([]msgpack.RawMessage, 0, respArrayLen-1)
	for i := 1; i < respArrayLen; i++ {
		elem, err := d.DecodeRaw()
		if err != nil {
			return fmt.Errorf("failed to decode into msgpack.RawMessage element #%d of response array", i-1)
		}
		r.CallResp.rawMessages = append(r.CallResp.rawMessages, elem)
	}

	return nil
}

type assertError struct {
	Code     int         `msgpack:"code"`
	BaseType string      `msgpack:"base_type"`
	Type     string      `msgpack:"type"`
	Message  string      `msgpack:"message"`
	Trace    interface{} `msgpack:"trace"`
}

func (s assertError) Error() string {
	// Just print struct as is, use hack with alias type to avoid recursion:
	// %v attempts to call Error() method for s, which is recursion.
	// This alias doesn't have method Error().
	type alias assertError
	return fmt.Sprintf("%+v", alias(s))
}

type StorageCallVShardError struct {
	BucketID uint64 `msgpack:"bucket_id"`
	Reason   string `msgpack:"reason"`
	Code     int    `msgpack:"code"`
	Type     string `msgpack:"type"`
	Message  string `msgpack:"message"`
	Name     string `msgpack:"name"`
	// These 3 fields below are send as string by vshard storage, so we decode them into string, not uuid.UUID type
	// Example: 00000000-0000-0002-0002-000000000000
	MasterUUID     string `msgpack:"master"`
	ReplicasetUUID string `msgpack:"replicaset"`
	ReplicaUUID    string `msgpack:"replica"`
	Destination    string `msgpack:"destination"`
}

func (s StorageCallVShardError) Error() string {
	// Just print struct as is, use hack with alias type to avoid recursion:
	// %v attempts to call Error() method for s, which is recursion.
	// This alias doesn't have method Error().
	type alias StorageCallVShardError
	return fmt.Sprintf("%+v", alias(s))
}

type StorageResultTypedFunc = func(result ...interface{}) error

type CallOpts struct {
	VshardMode VshardMode // vshard mode in call
	PoolMode   pool.Mode
	Timeout    time.Duration
}

// VshardRouterCallMode is a type to represent call mode for Router.Call method.
type VshardRouterCallMode int

const (
	// VshardRouterCallModeRO sets a read-only mode for Router.Call.
	VshardRouterCallModeRO VshardRouterCallMode = iota
	// VshardRouterCallModeRW sets a read-write mode for Router.Call.
	VshardRouterCallModeRW
	// VshardRouterCallModeRE acts like VshardRouterCallModeRO
	// with preference for a replica rather than a master.
	// This mode is not supported yet.
	VshardRouterCallModeRE
	// VshardRouterCallModeBRO acts like VshardRouterCallModeRO with balancing.
	VshardRouterCallModeBRO
	// VshardRouterCallModeBRE acts like VshardRouterCallModeRO with balancing
	// and preference for a replica rather than a master.
	VshardRouterCallModeBRE
)

// VshardRouterCallOptions represents options to Router.Call[XXX] methods.
type VshardRouterCallOptions struct {
	Timeout time.Duration
}

// VshardRouterCallResp represents a response from Router.Call[XXX] methods.
type VshardRouterCallResp struct {
	rawMessages []msgpack.RawMessage
}

// Get returns a response from user defined function as []interface{}.
func (r VshardRouterCallResp) Get() ([]interface{}, error) {
	resp := make([]interface{}, len(r.rawMessages))
	return resp, r.GetTyped(resp)
}

// GetTyped decodes a response from user defined function into custom values.
func (r VshardRouterCallResp) GetTyped(result []interface{}) error {
	minLen := len(result)
	if dataLen := len(r.rawMessages); dataLen < minLen {
		minLen = dataLen
	}

	for i := 0; i < minLen; i++ {
		if err := msgpack.Unmarshal(r.rawMessages[i], &result[i]); err != nil {
			return fmt.Errorf("failed to decode into result[%d] element #%d of response array: %w", i, i, err)
		}
	}

	return nil
}

// RouterCallImpl Perform shard operation function will restart operation
// after wrong bucket response until timeout is reached
// Deprecated: RouterCallImpl is deprecated.
// See https://github.com/KaymeKaydex/go-vshard-router/issues/110.
// Use Call method with RO, RW, RE, BRO, BRE modes instead.
func (r *Router) RouterCallImpl(ctx context.Context,
	bucketID uint64,
	opts CallOpts,
	fnc string,
	args interface{}) (interface{}, StorageResultTypedFunc, error) {

	var vshardCallOpts = VshardRouterCallOptions{
		Timeout: opts.Timeout,
	}

	var vshardCallMode VshardRouterCallMode

	switch opts.VshardMode {
	case WriteMode:
		vshardCallMode = VshardRouterCallModeRW
	case ReadMode:
		switch opts.PoolMode {
		case pool.ANY:
			vshardCallMode = VshardRouterCallModeBRO
		case pool.RO:
			vshardCallMode = VshardRouterCallModeRO
		case pool.RW:
			return nil, nil, fmt.Errorf("unexpected opts %+v", opts)
		case pool.PreferRO:
			vshardCallMode = VshardRouterCallModeBRE
		case pool.PreferRW:
			return nil, nil, fmt.Errorf("unexpected opts %+v", opts)
		default:
			return nil, nil, fmt.Errorf("unexpected opts.PoolMode %v", opts.PoolMode)
		}
	default:
		return nil, nil, fmt.Errorf("unexpected opts.VshardMode %v", opts.VshardMode)
	}

	vshardCallResp, err := r.Call(ctx, bucketID, vshardCallMode, fnc, args, vshardCallOpts)
	if err != nil {
		return nil, nil, err
	}

	data, err := vshardCallResp.Get()
	if err != nil {
		return nil, nil, err
	}

	return data, func(result ...interface{}) error {
		return vshardCallResp.GetTyped(result)
	}, nil
}

// Call calls the function identified by 'fnc' on the shard storing the bucket identified by 'bucket_id'.
func (r *Router) Call(ctx context.Context, bucketID uint64, mode VshardRouterCallMode,
	fnc string, args interface{}, opts VshardRouterCallOptions) (VshardRouterCallResp, error) {
	const vshardStorageClientCall = "vshard.storage.call"

	if bucketID < 1 || r.cfg.TotalBucketCount < bucketID {
		return VshardRouterCallResp{}, fmt.Errorf("bucket id is out of range: %d (total %d)", bucketID, r.cfg.TotalBucketCount)
	}

	var poolMode pool.Mode
	var vshardMode VshardMode

	switch mode {
	case VshardRouterCallModeRO:
		poolMode, vshardMode = pool.RO, ReadMode
	case VshardRouterCallModeRW:
		poolMode, vshardMode = pool.RW, WriteMode
	case VshardRouterCallModeRE:
		// poolMode, vshardMode = pool.PreferRO, ReadMode
		// since go-tarantool always use balance=true politic,
		// we can't support this case until: https://github.com/tarantool/go-tarantool/issues/400
		return VshardRouterCallResp{}, fmt.Errorf("mode VshardCallModeRE is not supported yet")
	case VshardRouterCallModeBRO:
		poolMode, vshardMode = pool.ANY, ReadMode
	case VshardRouterCallModeBRE:
		poolMode, vshardMode = pool.PreferRO, ReadMode
	default:
		return VshardRouterCallResp{}, fmt.Errorf("unknown VshardCallMode(%d)", mode)
	}

	timeout := callTimeoutDefault
	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tntReq := tarantool.NewCallRequest(vshardStorageClientCall).
		Context(ctx).
		Args([]interface{}{
			bucketID,
			vshardMode,
			fnc,
			args,
		})

	requestStartTime := time.Now()

	var err error

	for {
		if spent := time.Since(requestStartTime); spent > timeout {
			r.metrics().RequestDuration(spent, false, false)

			r.log().Debugf(ctx, "Return result on timeout; spent %s of timeout %s", spent, timeout)
			if err == nil {
				err = fmt.Errorf("cant get call cause call impl timeout")
			}

			return VshardRouterCallResp{}, err
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

		r.log().Infof(ctx, "Try call %s on replicaset %s for bucket %d", fnc, rs.info.Name, bucketID)

		var storageCallResponse vshardStorageCallResponseProto
		err = rs.conn.Do(tntReq, poolMode).GetTyped(&storageCallResponse)
		if err != nil {
			return VshardRouterCallResp{}, fmt.Errorf("got error on future.GetTyped(): %w", err)
		}

		r.log().Debugf(ctx, "Got call result response data %+v", storageCallResponse)

		if storageCallResponse.AssertError != nil {
			return VshardRouterCallResp{}, fmt.Errorf("%s: %s failed: %+v", vshardStorageClientCall, fnc, storageCallResponse.AssertError)
		}

		if storageCallResponse.VshardError != nil {
			vshardError := storageCallResponse.VshardError

			switch vshardError.Name {
			case VShardErrNameWrongBucket, VShardErrNameBucketIsLocked:
				// We reproduce here behavior in https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L663
				r.BucketReset(bucketID)

				if vshardError.Destination != "" {
					destinationUUID, err := uuid.Parse(vshardError.Destination)
					if err != nil {
						return VshardRouterCallResp{}, fmt.Errorf("protocol violation %s: malformed destination %w: %w",
							vshardStorageClientCall, vshardError, err)
					}

					var loggedOnce bool
					for {
						idToReplicasetRef := r.getIDToReplicaset()
						if _, ok := idToReplicasetRef[destinationUUID]; ok {
							_, err := r.BucketSet(bucketID, destinationUUID)
							if err == nil {
								break // breaks loop
							}
							r.log().Warnf(ctx, "Failed set bucket %d to %v (possible race): %v", bucketID, destinationUUID, err)
						}

						if !loggedOnce {
							r.log().Warnf(ctx, "Replicaset '%v' was not found, but received from storage as destination - please "+
								"update configuration", destinationUUID)
							loggedOnce = true
						}

						const defaultPoolingPause = 50 * time.Millisecond
						time.Sleep(defaultPoolingPause)

						if spent := time.Since(requestStartTime); spent > timeout {
							return VshardRouterCallResp{}, vshardError
						}
					}
				}

				// retry for VShardErrNameWrongBucket, VShardErrNameBucketIsLocked

				r.metrics().RetryOnCall("bucket_migrate")

				r.log().Debugf(ctx, "Retrying fnc '%s' cause got vshard error: %v", fnc, vshardError)

				// this vshardError will be returned to a caller in case of timeout
				err = vshardError
				continue
			case VShardErrNameTransferIsInProgress:
				// Since lua vshard router doesn't retry here, we don't retry too.
				// There is a comment why lua vshard router doesn't retry:
				// https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L697
				r.BucketReset(bucketID)
				return VshardRouterCallResp{}, vshardError
			case VShardErrNameNonMaster:
				// vshard.storage has returned NON_MASTER error, lua vshard router updates info about master in this case:
				// See: https://github.com/tarantool/vshard/blob/b6fdbe950a2e4557f05b83bd8b846b126ec3724e/vshard/router/init.lua#L704.
				// Since we use go-tarantool library, and go-tarantool library doesn't provide API to update info about current master,
				// we just return this error as is.
				return VshardRouterCallResp{}, vshardError
			default:
				return VshardRouterCallResp{}, vshardError
			}
		}

		r.metrics().RequestDuration(time.Since(requestStartTime), true, false)

		return storageCallResponse.CallResp, nil
	}
}

// CallRO is an alias for Call with VshardRouterCallModeRO.
func (r *Router) CallRO(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts VshardRouterCallOptions) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, VshardRouterCallModeRO, fnc, args, opts)
}

// CallRW is an alias for Call with VshardRouterCallModeRW.
func (r *Router) CallRW(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts VshardRouterCallOptions) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, VshardRouterCallModeRW, fnc, args, opts)
}

// CallRE is an alias for Call with VshardRouterCallModeRE.
func (r *Router) CallRE(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts VshardRouterCallOptions) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, VshardRouterCallModeRE, fnc, args, opts)
}

// CallBRO is an alias for Call with VshardRouterCallModeBRO.
func (r *Router) CallBRO(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts VshardRouterCallOptions) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, VshardRouterCallModeBRO, fnc, args, opts)
}

// CallBRE is an alias for Call with VshardRouterCallModeBRE.
func (r *Router) CallBRE(ctx context.Context, bucketID uint64,
	fnc string, args interface{}, opts VshardRouterCallOptions) (VshardRouterCallResp, error) {
	return r.Call(ctx, bucketID, VshardRouterCallModeBRE, fnc, args, opts)
}

// RouterMapCallRWOptions sets options for RouterMapCallRW.
type RouterMapCallRWOptions struct {
	// Timeout defines timeout for RouterMapCallRW.
	Timeout time.Duration
}

type storageMapResponseProto[T any] struct {
	ok    bool
	value T
	err   StorageCallVShardError
}

func (r *storageMapResponseProto[T]) DecodeMsgpack(d *msgpack.Decoder) error {
	// proto for 'storage_map' method
	// https://github.com/tarantool/vshard/blob/8d299bfecff8bc656056658350ad48c829f9ad3f/vshard/storage/init.lua#L3158
	respArrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if respArrayLen == 0 {
		return fmt.Errorf("protocol violation: invalid array length: %d", respArrayLen)
	}

	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	if code == msgpcode.Nil {
		err = d.DecodeNil()
		if err != nil {
			return err
		}

		if respArrayLen != 2 {
			return fmt.Errorf("protocol violation: length is %d on vshard error case", respArrayLen)
		}

		err = d.Decode(&r.err)
		if err != nil {
			return fmt.Errorf("failed to decode storage vshard error: %w", err)
		}

		return nil
	}

	isOk, err := d.DecodeBool()
	if err != nil {
		return err
	}

	if !isOk {
		return fmt.Errorf("protocol violation: isOk=false")
	}

	switch respArrayLen {
	case 1:
		break
	case 2:
		err = d.Decode(&r.value)
		if err != nil {
			return fmt.Errorf("can't decode value %T: %w", r.value, err)
		}
	default:
		return fmt.Errorf("protocol violation: invalid array length when no vshard error: %d", respArrayLen)
	}

	r.ok = true

	return nil
}

type storageRefResponseProto struct {
	err         error
	bucketCount uint64
}

func (r *storageRefResponseProto) DecodeMsgpack(d *msgpack.Decoder) error {
	respArrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if respArrayLen == 0 {
		return fmt.Errorf("protocol violation: invalid array length: %d", respArrayLen)
	}

	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	if code == msgpcode.Nil {
		err = d.DecodeNil()
		if err != nil {
			return err
		}

		if respArrayLen != 2 {
			return fmt.Errorf("protocol violation: length is %d on error case", respArrayLen)
		}

		// The possible variations of error here are fully unknown yet for us, e.g:
		// vshard error, assert error or some other type of error. So this question requires research.
		// So we do not decode it to some known error format, because we don't use it anyway.
		decodedError, err := d.DecodeInterface()
		if err != nil {
			return err
		}

		// convert empty interface into error
		r.err = fmt.Errorf("%v", decodedError)

		return nil
	}

	r.bucketCount, err = d.DecodeUint64()
	if err != nil {
		return err
	}

	return nil
}

// RouterMapCallRWImpl perform call function on all masters in the cluster
// with a guarantee that in case of success it was executed with all
// buckets being accessible for reads and writes.
// Deprecated: RouterMapCallRWImpl is deprecated.
// Use more general RouterMapCallRW instead.
func (r *Router) RouterMapCallRWImpl(
	ctx context.Context,
	fnc string,
	args interface{},
	opts CallOpts,
) (map[uuid.UUID]interface{}, error) {
	return RouterMapCallRW[interface{}](r, ctx, fnc, args, RouterMapCallRWOptions{Timeout: opts.Timeout})
}

// RouterMapCallRW is a consistent Map-Reduce. The given function is called on all masters in the
// cluster with a guarantee that in case of success it was executed with all
// buckets being accessible for reads and writes.
// T is a return type of user defined function 'fnc'.
// We define it as a distinct function, not a Router method, because golang limitations,
// see: https://github.com/golang/go/issues/49085.
func RouterMapCallRW[T any](r *Router, ctx context.Context,
	fnc string, args interface{}, opts RouterMapCallRWOptions,
) (map[uuid.UUID]T, error) {
	const vshardStorageServiceCall = "vshard.storage._call"

	timeout := callTimeoutDefault
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
		var storageRefResponse storageRefResponseProto

		if err := rsFuture.future.GetTyped(&storageRefResponse); err != nil {
			return nil, fmt.Errorf("rs {%s} storage_ref err: %v", rsFuture.uuid, err)
		}

		if storageRefResponse.err != nil {
			return nil, fmt.Errorf("storage_ref failed on %v: %v", rsFuture.uuid, storageRefResponse.err)
		}

		totalBucketCount += storageRefResponse.bucketCount
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
	idToResult := make(map[uuid.UUID]T)
	for _, rsFuture := range rsFutures {
		var storageMapResponse storageMapResponseProto[T]

		err := rsFuture.future.GetTyped(&storageMapResponse)
		if err != nil {
			return nil, fmt.Errorf("rs {%s} storage_map err: %v", rsFuture.uuid, err)
		}

		if !storageMapResponse.ok {
			return nil, fmt.Errorf("storage_map failed on %v: %+v", rsFuture.uuid, storageMapResponse.err)
		}

		idToResult[rsFuture.uuid] = storageMapResponse.value
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

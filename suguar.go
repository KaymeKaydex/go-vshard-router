package vshard_router //nolint:revive

import (
	"context"
	"fmt"

	"github.com/tarantool/go-tarantool/v2/pool"
)

// CallRequest helps you to create a call request object for execution
// by a Connection.
type CallRequest struct {
	ctx      context.Context
	fnc      string
	args     interface{}
	bucketID uint64
}

// CallResponse is a backwards-compatible structure with go-tarantool for easier replacement.
type CallResponse struct {
	rawResp     interface{}
	getTypedFnc StorageResultTypedFunc
	err         error
}

// NewCallRequest returns a new empty CallRequest.
func (r *Router) NewCallRequest(function string) *CallRequest {
	req := new(CallRequest)
	req.fnc = function
	return req
}

// Do perform a request asynchronously on the connection.
func (r *Router) Do(req *CallRequest, userMode pool.Mode) *CallResponse {
	ctx := req.ctx
	bucketID := req.bucketID
	resp := new(CallResponse)

	if req.bucketID == 0 {
		if r.cfg.BucketGetter == nil {
			resp.err = fmt.Errorf("bucket id for request is not set")
			return resp
		}

		bucketID = r.cfg.BucketGetter(ctx)
	}

	vshardMode := ReadMode

	if userMode == pool.RW {
		vshardMode = WriteMode
	}

	resp.rawResp, resp.getTypedFnc, resp.err = r.RouterCallImpl(ctx,
		bucketID,
		CallOpts{
			Timeout:    r.cfg.RequestTimeout,
			PoolMode:   userMode,
			VshardMode: vshardMode,
		},
		req.fnc,
		req.args)

	return resp
}

// Args sets the args for the eval request.
// Note: default value is empty.
func (req *CallRequest) Args(args interface{}) *CallRequest {
	req.args = args
	return req
}

// Context sets a passed context to the request.
func (req *CallRequest) Context(ctx context.Context) *CallRequest {
	req.ctx = ctx
	return req
}

// BucketID method that sets the bucketID for your request.
// You can ignore this parameter if you have a bucketGetter.
// However, this method has a higher priority.
func (req *CallRequest) BucketID(bucketID uint64) *CallRequest {
	req.bucketID = bucketID
	return req
}

// GetTyped waits for Future and calls msgpack.Decoder.Decode(result) if no error happens.
func (resp *CallResponse) GetTyped(result interface{}) error {
	if resp.err != nil {
		return resp.err
	}

	return resp.getTypedFnc(result)
}

// Get waits for Future to be filled and returns the data of the Response and error.
func (resp *CallResponse) Get() ([]interface{}, error) {
	if resp.err != nil {
		return nil, resp.err
	}

	return []interface{}{resp.rawResp}, nil
}

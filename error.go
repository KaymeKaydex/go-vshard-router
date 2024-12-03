package vshard_router //nolint:revive

import "fmt"

// VShard error codes
const (
	VShardErrCodeWrongBucket            = 1
	VShardErrCodeNonMaster              = 2
	VShardErrCodeBucketAlreadyExists    = 3
	VShardErrCodeNoSuchReplicaset       = 4
	VShardErrCodeMoveToSelf             = 5
	VShardErrCodeMissingMaster          = 6
	VShardErrCodeTransferIsInProgress   = 7
	VShardErrCodeUnreachableReplicaset  = 8
	VShardErrCodeNoRouteToBucket        = 9
	VShardErrCodeNonEmpty               = 10
	VShardErrCodeUnreachableMaster      = 11
	VShardErrCodeOutOfSync              = 12
	VShardErrCodeHighReplicationLag     = 13
	VShardErrCodeUnreachableReplica     = 14
	VShardErrCodeLowRedundancy          = 15
	VShardErrCodeInvalidRebalancing     = 16
	VShardErrCodeSuboptimalReplica      = 17
	VShardErrCodeUnknownBuckets         = 18
	VShardErrCodeReplicasetIsLocked     = 19
	VShardErrCodeObjectIsOutdated       = 20
	VShardErrCodeRouterAlreadyExists    = 21
	VShardErrCodeBucketIsLocked         = 22
	VShardErrCodeInvalidCfg             = 23
	VShardErrCodeBucketIsPinned         = 24
	VShardErrCodeTooManyReceiving       = 25
	VShardErrCodeStorageIsReferenced    = 26
	VShardErrCodeStorageRefAdd          = 27
	VShardErrCodeStorageRefUse          = 28
	VShardErrCodeStorageRefDel          = 29
	VShardErrCodeBucketRecvDataError    = 30
	VShardErrCodeMultipleMastersFound   = 31
	VShardErrCodeReplicasetInBackoff    = 32
	VShardErrCodeStorageIsDisabled      = 33
	VShardErrCodeBucketIsCorrupted      = 34
	VShardErrCodeRouterIsDisabled       = 35
	VShardErrCodeBucketGCError          = 36
	VShardErrCodeStorageCfgIsInProgress = 37
	VShardErrCodeRouterCfgIsInProgress  = 38
	VShardErrCodeBucketInvalidUpdate    = 39
	VShardErrCodeVhandshakeNotComplete  = 40
	VShardErrCodeInstanceNameMismatch   = 41
)

// VShard error names
const (
	VShardErrNameWrongBucket            = "WRONG_BUCKET"
	VShardErrNameNonMaster              = "NON_MASTER"
	VShardErrNameBucketAlreadyExists    = "BUCKET_ALREADY_EXISTS"
	VShardErrNameNoSuchReplicaset       = "NO_SUCH_REPLICASET"
	VShardErrNameMoveToSelf             = "MOVE_TO_SELF"
	VShardErrNameMissingMaster          = "MISSING_MASTER"
	VShardErrNameTransferIsInProgress   = "TRANSFER_IS_IN_PROGRESS"
	VShardErrNameUnreachableReplicaset  = "UNREACHABLE_REPLICASET"
	VShardErrNameNoRouteToBucket        = "NO_ROUTE_TO_BUCKET"
	VShardErrNameNonEmpty               = "NON_EMPTY"
	VShardErrNameUnreachableMaster      = "UNREACHABLE_MASTER"
	VShardErrNameOutOfSync              = "OUT_OF_SYNC"
	VShardErrNameHighReplicationLag     = "HIGH_REPLICATION_LAG"
	VShardErrNameUnreachableReplica     = "UNREACHABLE_REPLICA"
	VShardErrNameLowRedundancy          = "LOW_REDUNDANCY"
	VShardErrNameInvalidRebalancing     = "INVALID_REBALANCING"
	VShardErrNameSuboptimalReplica      = "SUBOPTIMAL_REPLICA"
	VShardErrNameUnknownBuckets         = "UNKNOWN_BUCKETS"
	VShardErrNameReplicasetIsLocked     = "REPLICASET_IS_LOCKED"
	VShardErrNameObjectIsOutdated       = "OBJECT_IS_OUTDATED"
	VShardErrNameRouterAlreadyExists    = "ROUTER_ALREADY_EXISTS"
	VShardErrNameBucketIsLocked         = "BUCKET_IS_LOCKED"
	VShardErrNameInvalidCfg             = "INVALID_CFG"
	VShardErrNameBucketIsPinned         = "BUCKET_IS_PINNED"
	VShardErrNameTooManyReceiving       = "TOO_MANY_RECEIVING"
	VShardErrNameStorageIsReferenced    = "STORAGE_IS_REFERENCED"
	VShardErrNameStorageRefAdd          = "STORAGE_REF_ADD"
	VShardErrNameStorageRefUse          = "STORAGE_REF_USE"
	VShardErrNameStorageRefDel          = "STORAGE_REF_DEL"
	VShardErrNameBucketRecvDataError    = "BUCKET_RECV_DATA_ERROR"
	VShardErrNameMultipleMastersFound   = "MULTIPLE_MASTERS_FOUND"
	VShardErrNameReplicasetInBackoff    = "REPLICASET_IN_BACKOFF"
	VShardErrNameStorageIsDisabled      = "STORAGE_IS_DISABLED"
	VShardErrNameBucketIsCorrupted      = "BUCKET_IS_CORRUPTED"
	VShardErrNameRouterIsDisabled       = "ROUTER_IS_DISABLED"
	VShardErrNameBucketGCError          = "BUCKET_GC_ERROR"
	VShardErrNameStorageCfgIsInProgress = "STORAGE_CFG_IS_IN_PROGRESS"
	VShardErrNameRouterCfgIsInProgress  = "ROUTER_CFG_IS_IN_PROGRESS"
	VShardErrNameBucketInvalidUpdate    = "BUCKET_INVALID_UPDATE"
	VShardErrNameVhandshakeNotComplete  = "VHANDSHAKE_NOT_COMPLETE"
	VShardErrNameInstanceNameMismatch   = "INSTANCE_NAME_MISMATCH"
)

type bucketStatError struct {
	BucketID uint64 `msgpack:"bucket_id"`
	Reason   string `msgpack:"reason"`
	Code     int    `msgpack:"code"`
	Type     string `msgpack:"type"`
	Message  string `msgpack:"message"`
	Name     string `msgpack:"name"`
}

func (bse bucketStatError) Error() string {
	type alias bucketStatError
	return fmt.Sprintf("%+v", alias(bse))
}

func newVShardErrorNoRouteToBucket(bucketID uint64) error {
	return &vshardError{
		Name:     VShardErrNameNoRouteToBucket,
		Code:     VShardErrCodeNoRouteToBucket,
		Type:     "ShardingError",
		BucketID: bucketID,
		Message:  fmt.Sprintf("Bucket %d cannot be found. Is rebalancing in progress?", bucketID),
	}
}

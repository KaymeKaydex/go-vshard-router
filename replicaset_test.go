package vshard_router

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestReplicasetInfo_String(t *testing.T) {
	rsUUID := uuid.New()
	rsInfo := ReplicasetInfo{
		Name: "test",
		UUID: rsUUID,
	}

	rs := Replicaset{
		info: rsInfo,
	}

	require.Equal(t, rsInfo.String(), rs.String())
	require.Contains(t, rsInfo.String(), "test")
	require.Contains(t, rsInfo.String(), rsUUID.String())
}

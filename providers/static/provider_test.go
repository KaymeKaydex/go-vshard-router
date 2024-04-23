package static

import (
	"testing"

	"github.com/stretchr/testify/require"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
)

func TestNewProvider(t *testing.T) {
	tCases := []struct {
		Source map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo
	}{
		{nil},
		{make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)},
	}

	for _, tc := range tCases {
		t.Run("provider", func(t *testing.T) {
			if tc.Source == nil || len(tc.Source) == 0 {
				require.Panics(t, func() {
					NewProvider(tc.Source)
				})
			}

		})
	}
}

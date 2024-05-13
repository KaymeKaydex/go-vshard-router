package vshard_router

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSearchLock_WaitOnSearch(t *testing.T) {
	lock := searchLock{
		mu:        sync.RWMutex{},
		perBucket: make([]chan struct{}, 10),
	}

	noLockStart := time.Now()
	lock.WaitOnSearch(5)
	require.True(t, time.Since(noLockStart) < time.Millisecond)

	lockStart := time.Now()
	chStopSearch := lock.StartSearch(3)

	go func() {
		time.Sleep(time.Millisecond * 10)
		close(chStopSearch)
	}()

	noLockStart = time.Now()
	lock.WaitOnSearch(5)
	require.True(t, time.Since(noLockStart) < time.Millisecond)

	lock.WaitOnSearch(3)

	require.True(t, time.Since(lockStart) < 12*time.Millisecond && time.Since(lockStart) > 9*time.Millisecond)
}

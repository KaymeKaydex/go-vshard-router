package tnt_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	vshardrouter "github.com/KaymeKaydex/go-vshard-router"
)

type concurrentTopologyProvider struct {
	done   chan struct{}
	closed chan struct{}
}

func (c *concurrentTopologyProvider) Init(tc vshardrouter.TopologyController) error {
	ctx := context.Background()
	cfg := getCfg()

	if err := tc.AddReplicasets(ctx, cfg); err != nil {
		panic(err)
	}

	c.done = make(chan struct{})
	c.closed = make(chan struct{})

	added := cfg
	removed := make(map[vshardrouter.ReplicasetInfo][]vshardrouter.InstanceInfo)

	go func() {
		defer close(c.closed)
		//nolint:errcheck
		defer tc.AddReplicasets(ctx, removed)

		type actiont int

		const add actiont = 0
		const remove actiont = 1

		for {
			select {
			case <-c.done:
				return
			default:
			}

			canAdd := len(removed) > 0
			canRemove := len(added) > 0

			var action actiont

			switch {
			case canAdd && canRemove:
				//nolint:gosec
				action = actiont(rand.Int() % 2)
			case canAdd:
				action = add
			case canRemove:
				action = remove
			default:
				panic(fmt.Sprintf("unreachable case: %v, %v", added, removed))
			}

			switch action {
			case add:
				var keys []vshardrouter.ReplicasetInfo
				for k := range removed {
					keys = append(keys, k)
				}
				//nolint:gosec
				key := keys[rand.Int()%len(keys)]

				added[key] = removed[key]
				delete(removed, key)

				_ = tc.AddReplicaset(ctx, key, added[key])
			case remove:
				var keys []vshardrouter.ReplicasetInfo
				for k := range added {
					keys = append(keys, k)
				}
				//nolint:gosec
				key := keys[rand.Int()%len(keys)]

				removed[key] = added[key]
				delete(added, key)

				_ = tc.RemoveReplicaset(ctx, key.UUID)
			default:
				panic("unreachable case")
			}
		}
	}()

	return nil
}

func (c *concurrentTopologyProvider) Close() {
	close(c.done)
	<-c.closed
}

func TestConncurrentTopologyChange(t *testing.T) {
	/* What we do:
	1) Addreplicaset + Removereplicaset by random in one goroutine
	2) Call ReplicaCall, MapRw and etc. in another goroutines
	*/

	if !isCorrectRun() {
		log.Printf("Incorrect run of tnt-test framework")
		return
	}

	// suppress the below linter warning:
	// unused-parameter: parameter 't' seems to be unused, consider removing or renaming it as _ (revive)
	_ = t

	// Don't run this parallel with other tests, because this test is heavy and used to detect data races.
	// Therefore this test may impact other ones.
	// t.Parallel()

	tc := &concurrentTopologyProvider{}

	router, err := vshardrouter.NewRouter(context.Background(), vshardrouter.Config{
		TopologyProvider: tc,
		DiscoveryTimeout: 5 * time.Second,
		DiscoveryMode:    vshardrouter.DiscoveryModeOn,
		TotalBucketCount: totalBucketCount,
		User:             defaultTntUser,
		Password:         defaultTntPassword,
	})
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			//nolint:gosec
			bucketID := uint64((rand.Int() % totalBucketCount) + 1)
			args := []interface{}{"arg1"}

			callOpts := vshardrouter.CallOpts{
				VshardMode: vshardrouter.ReadMode,
			}

			_, _, _ = router.RouterCallImpl(ctx, bucketID, callOpts, "echo", args)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			args := []interface{}{"arg1"}
			_, _ = router.RouterMapCallRWImpl(ctx, "echo", args, vshardrouter.CallOpts{})
		}
	}()

	wg.Wait()

	// is router.Close method required?
	tc.Close()
}

// nolint:revive
package test_helper

import (
	"context"

	ttnt "github.com/tarantool/go-tarantool/v2/test_helpers"
	"golang.org/x/sync/errgroup"
)

func StartTarantoolInstances(instsOpts []ttnt.StartOpts) ([]ttnt.TarantoolInstance, error) {
	ctx := context.Background()
	instances := make([]ttnt.TarantoolInstance, len(instsOpts))
	errGr, _ := errgroup.WithContext(ctx)

	for i, opts := range instsOpts {
		opts := opts
		i := i
		errGr.Go(func() error {
			instance, err := ttnt.StartTarantool(opts)
			instances[i] = instance

			return err
		})

	}
	err := errGr.Wait()

	return instances, err
}

package bspgraph

import (
	"errors"

	"github.com/valkyraycho/links-r-us/bspgraph/message"
)

type GraphConfig struct {
	QueueFactory   message.QueueFactory
	ComputeFn      ComputeFunc
	ComputeWorkers int
}

func (g *GraphConfig) validate() error {
	if g.QueueFactory == nil {
		g.QueueFactory = message.NewInMemoryQueue
	}
	if g.ComputeWorkers <= 0 {
		g.ComputeWorkers = 1
	}

	if g.ComputeFn == nil {
		return errors.New("compute function not specified")
	}
	return nil
}

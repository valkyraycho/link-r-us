package pagerank

import (
	"context"
	"fmt"

	"github.com/valkyraycho/links-r-us/bspgraph"
	"github.com/valkyraycho/links-r-us/bspgraph/aggregator"
)

type Calculator struct {
	g   *bspgraph.Graph
	cfg Config

	executorFactory bspgraph.ExecutorFactory
}

func (c *Calculator) SetExecutorFactory(factory bspgraph.ExecutorFactory) {
	c.executorFactory = factory
}

func NewCalculator(cfg Config) (*Calculator, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("PageRank calculator config validation failed: %w", err)
	}

	g, err := bspgraph.NewGraph(bspgraph.GraphConfig{
		ComputeWorkers: cfg.ComputeWorkers,
		ComputeFn:      makeComputeFunc(cfg.DampingFactor),
	})

	if err != nil {
		return nil, err
	}

	return &Calculator{
		g:               g,
		cfg:             cfg,
		executorFactory: bspgraph.NewExecutor,
	}, nil
}

func (c *Calculator) AddVertex(id string) {
	c.g.AddVertex(id, 0.0)
}

func (c *Calculator) AddEdge(src, dst string) error {
	if src == dst {
		return nil
	}

	return c.g.AddEdge(src, dst, nil)
}

func (c *Calculator) Graph() *bspgraph.Graph {
	return c.g
}

func (c *Calculator) Scores(visitFn func(id string, score float64) error) error {
	for id, v := range c.g.Vertices() {
		if err := visitFn(id, v.Value().(float64)); err != nil {
			return err
		}
	}

	return nil
}

// Executor creates and return a bspgraph.Executor for running the PageRank
// algorithm once the graph layout has been properly set up.
func (c *Calculator) Executor() *bspgraph.Executor {
	c.registerAggregators()
	cb := bspgraph.ExecutorCallbacks{
		PreStep: func(_ context.Context, g *bspgraph.Graph) error {
			g.Aggregator("SAD").Set(0.0)
			g.Aggregator(residualOutputAccName(g.Superstep())).Set(0.0)
			return nil
		},
		PostStepKeepRunning: func(ctx context.Context, g *bspgraph.Graph, activeInStep int) (bool, error) {
			sad := g.Aggregator("SAD").Get().(float64)
			return !(g.Superstep() > 1 && sad < c.cfg.MinSADForConvergence), nil
		},
	}
	return c.executorFactory(c.g, cb)
}

// registerAggregators creates and registers the aggregator instances that we
// need to run the PageRank calculation algorithm.
func (c *Calculator) registerAggregators() {
	c.g.RegisterAggregator("page_count", new(aggregator.IntAccumulator))
	c.g.RegisterAggregator("residual_0", new(aggregator.Float64Accumulator))
	c.g.RegisterAggregator("residual_1", new(aggregator.Float64Accumulator))
	c.g.RegisterAggregator("SAD", new(aggregator.Float64Accumulator))
}

// residualOutputAccName returns the name of the accumulator where the
// residual PageRank scores for the specified superstep are to be written to.
func residualOutputAccName(superstep int) string {
	if superstep%2 == 0 {
		return "residual_0"
	}
	return "residual_1"
}

// residualInputAccName returns the name of the accumulator where the
// residual PageRank scores for the specified superstep are to be read from.
func residualInputAccName(superstep int) string {
	if (superstep+1)%2 == 0 {
		return "residual_0"
	}
	return "residual_1"
}

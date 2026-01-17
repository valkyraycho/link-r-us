package shortestpath

import (
	"math"

	"github.com/valkyraycho/links-r-us/bspgraph"
	"github.com/valkyraycho/links-r-us/bspgraph/message"
)

// Calculator implements a shortest path calculator from a single vertex to
// all other vertices in a connected graph.
type Calculator struct {
	g     *bspgraph.Graph
	srcID string

	executorFactory bspgraph.ExecutorFactory
}

func NewCalculator(numWorkers int) (*Calculator, error) {
	c := &Calculator{
		executorFactory: bspgraph.NewExecutor,
	}

	var err error
	if c.g, err = bspgraph.NewGraph(bspgraph.GraphConfig{
		ComputeFn:      c.findShortestPath,
		ComputeWorkers: numWorkers,
	}); err != nil {
		return nil, err
	}

	return c, nil
}

type PathCostMessage struct {
	FromID string
	Cost   int
}

func (pc PathCostMessage) Type() string { return "cost" }

type pathState struct {
	minDist    int
	prevInPath string
}

func (c *Calculator) findShortestPath(g *bspgraph.Graph, v *bspgraph.Vertex, msgIt message.Iterator) error {
	if g.Superstep() == 0 {
		v.SetValue(&pathState{
			minDist: int(math.MaxInt64),
		},
		)
	}

	minDist := int(math.MaxInt64)
	if v.ID() == c.srcID {
		minDist = 0
	}

	var via string

	for msgIt.Next() {
		m := msgIt.Message().(*PathCostMessage)
		if m.Cost < minDist {
			minDist = m.Cost
			via = m.FromID
		}
	}

	st := v.Value().(*pathState)
	if minDist < st.minDist {
		st.minDist = minDist
		st.prevInPath = via
		for _, e := range v.Edges() {
			costMsg := &PathCostMessage{
				FromID: v.ID(),
				Cost:   minDist + e.Value().(int),
			}

			if err := g.SendMessage(e.DstID(), costMsg); err != nil {
				return err
			}
		}
	}

	return nil
}

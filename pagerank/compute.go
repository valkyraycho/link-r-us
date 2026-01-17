package pagerank

import (
	"math"

	"github.com/valkyraycho/links-r-us/bspgraph"
	"github.com/valkyraycho/links-r-us/bspgraph/message"
)

// IncomingScoreMessage is used for distributing PageRank scores to neighbors.
type IncomingScoreMessage struct {
	Score float64
}

// Type returns the type of this message
func (pr IncomingScoreMessage) Type() string { return "score" }

func makeComputeFunc(dampingFactor float64) bspgraph.ComputeFunc {
	return func(g *bspgraph.Graph, v *bspgraph.Vertex, msgIt message.Iterator) error {
		superStep := g.Superstep()
		pageCountAgg := g.Aggregator("page_count")

		if superStep == 0 {
			pageCountAgg.Aggregate(1)
			return nil
		}

		var (
			pageCount = float64(pageCountAgg.Get().(int))
			newScore  float64
		)

		switch superStep {
		case 1:
			newScore = 1.0 / pageCount
		default:
			newScore = (1.0 - dampingFactor) / pageCount
			for msgIt.Next() {
				score := msgIt.Message().(IncomingScoreMessage).Score
				newScore += dampingFactor * score
			}

			resAggr := g.Aggregator(residualInputAccName(superStep))
			newScore += resAggr.Get().(float64)
		}

		absDelta := math.Abs(v.Value().(float64) - newScore)
		g.Aggregator("SAD").Aggregate(absDelta)

		v.SetValue(newScore)

		numOutLinks := float64(len(v.Edges()))
		if numOutLinks == 0.0 {
			g.Aggregator(residualOutputAccName(superStep)).Aggregate(newScore / pageCount)
			return nil
		}

		return g.BroadcastToNeighbors(v, IncomingScoreMessage{Score: newScore / numOutLinks})
	}
}

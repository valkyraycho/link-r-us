package color

import (
	"math/rand/v2"

	"github.com/valkyraycho/links-r-us/bspgraph"
	"github.com/valkyraycho/links-r-us/bspgraph/message"
)

type vertexState struct {
	token      int
	color      int
	usedColors map[int]bool
}

type VertexStateMessage struct {
	ID    string
	Token int
	Color int
}

func (m *VertexStateMessage) Type() string { return "VertexStateMessage" }

func (s *vertexState) asMessage(id string) *VertexStateMessage {
	return &VertexStateMessage{
		ID:    id,
		Token: s.token,
		Color: s.color,
	}
}

func assignColorsToGraph(g *bspgraph.Graph, v *bspgraph.Vertex, msgIt message.Iterator) error {
	v.Freeze()

	state := v.Value().(*vertexState)

	if g.Superstep() == 0 {
		if state.color == 0 && len(v.Edges()) == 0 {
			state.color = 1
			return nil
		}
		state.token = rand.Int()
		state.usedColors = make(map[int]bool)
		return g.BroadcastToNeighbors(v, state.asMessage(v.ID()))
	}

	if state.color != 0 {
		return nil
	}

	pickNextColor := true
	myID := v.ID()

	for msgIt.Next() {
		m := msgIt.Message().(*VertexStateMessage)

		if m.Color != 0 {
			state.usedColors[m.Color] = true
		} else if state.token < m.Token || (state.token == m.Token && myID < m.ID) {
			pickNextColor = false
		}
	}

	if !pickNextColor {
		return g.BroadcastToNeighbors(v, state.asMessage(v.ID()))
	}

	for nextColor := 1; ; nextColor++ {
		if state.usedColors[nextColor] {
			continue
		}

		state.color = nextColor
		return g.BroadcastToNeighbors(v, state.asMessage(v.ID()))
	}
}

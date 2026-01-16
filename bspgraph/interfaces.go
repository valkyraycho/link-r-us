package bspgraph

import "github.com/valkyraycho/links-r-us/bspgraph/message"

type Aggregator interface {
	Type() string
	Set(val any)
	Get() any
	Aggregate(val any)
	Delta() any
}

// Relayer is implemented by types that can relay messages to vertices that
// are managed by a remote graph instance.
type Relayer interface {
	// Relay a message to a vertex that is not known locally. Calls to
	// Relay must return ErrDestinationIsLocal if the provided dst value is
	// not a valid remote destination.
	Relay(dst string, msg message.Message) error
}

type RelayerFunc func(string, message.Message) error

func (f RelayerFunc) Relay(dst string, msg message.Message) error {
	return f(dst, msg)
}

type ComputeFunc func(g *Graph, v *Vertex, msgIt message.Iterator) error

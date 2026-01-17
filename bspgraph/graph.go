package bspgraph

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/valkyraycho/links-r-us/bspgraph/message"
)

var (
	// ErrUnknownEdgeSource is returned by AddEdge when the source vertex
	// is not present in the graph.
	ErrUnknownEdgeSource = errors.New("source vertex is not part of the graph")

	// ErrDestinationIsLocal is returned by Relayer instances to indicate
	// that a message destination is actually owned by the local graph.
	ErrDestinationIsLocal = errors.New("message destination is assigned to the local graph")

	// ErrInvalidMessageDestination is returned by calls to SendMessage and
	// BroadcastToNeighbors when the destination cannot be resolved to any
	// (local or remote) vertex.
	ErrInvalidMessageDestination = errors.New("invalid message destination")
)

type Vertex struct {
	id       string
	value    any
	active   bool
	msgQueue [2]message.Queue
	edges    []*Edge
}

func (v *Vertex) ID() string       { return v.id }
func (v *Vertex) Value() any       { return v.value }
func (v *Vertex) SetValue(val any) { v.value = val }
func (v *Vertex) Freeze()          { v.active = false }
func (v *Vertex) Edges() []*Edge   { return v.edges }

type Edge struct {
	value any
	dstID string
}

func (v *Edge) DstID() string    { return v.dstID }
func (v *Edge) Value() any       { return v.value }
func (v *Edge) SetValue(val any) { v.value = val }

type Graph struct {
	superstep int

	aggregators map[string]Aggregator
	vertices    map[string]*Vertex
	computeFn   ComputeFunc

	queueFactory message.QueueFactory
	relayer      Relayer

	wg              sync.WaitGroup
	vertexCh        chan *Vertex
	errCh           chan error
	stepCompletedCh chan struct{}
	activeInStep    int64
	pendingInStep   int64
}

func NewGraph(cfg GraphConfig) (*Graph, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("graph config validation failed: %w", err)
	}

	g := &Graph{
		computeFn:    cfg.ComputeFn,
		queueFactory: cfg.QueueFactory,
		aggregators:  make(map[string]Aggregator),
		vertices:     make(map[string]*Vertex),
	}
	g.startWorkers(cfg.ComputeWorkers)
	return g, nil
}

func (g *Graph) Superstep() int { return g.superstep }

func (g *Graph) Vertices() map[string]*Vertex { return g.vertices }

func (g *Graph) AddVertex(id string, initValue any) {
	v := g.vertices[id]
	if v == nil {
		v = &Vertex{
			id: id,
			msgQueue: [2]message.Queue{
				g.queueFactory(),
				g.queueFactory(),
			},
			active: true,
		}
	}

	v.SetValue(initValue)
}

func (g *Graph) AddEdge(srcID, dstID string, initValue any) error {
	srcVert := g.vertices[srcID]
	if srcVert == nil {
		return fmt.Errorf("create edge from %q to %q: %w", srcID, dstID, ErrUnknownEdgeSource)
	}

	srcVert.edges = append(srcVert.edges, &Edge{
		dstID: dstID,
		value: initValue,
	})
	return nil
}

func (g *Graph) RegisterAggregator(name string, aggr Aggregator) { g.aggregators[name] = aggr }

func (g *Graph) Aggregator(name string) Aggregator {
	return g.aggregators[name]
}

func (g *Graph) RegisterRelayer(relayer Relayer) { g.relayer = relayer }

func (g *Graph) BroadcastToNeighbors(v *Vertex, msg message.Message) error {
	for _, e := range v.edges {
		if err := g.SendMessage(e.dstID, msg); err != nil {
			return err
		}
	}

	return nil
}

func (g *Graph) SendMessage(dstID string, msg message.Message) error {
	dstVert := g.vertices[dstID]

	if dstVert != nil {
		queueIndex := (g.superstep + 1) % 2
		return dstVert.msgQueue[queueIndex].Enqueue(msg)
	}

	if g.relayer != nil {
		if err := g.relayer.Relay(dstID, msg); !errors.Is(err, ErrDestinationIsLocal) {
			return err
		}
	}

	return fmt.Errorf("message cannot be sent to %q: %w", dstID, ErrUnknownEdgeSource)
}

func (g *Graph) Reset() error {
	g.superstep = 0

	for _, v := range g.vertices {
		for i := range 2 {
			if err := v.msgQueue[i].Close(); err != nil {
				return fmt.Errorf("closing message queue #%d for vertex %v: %w", i, v.ID(), err)
			}
		}
	}
	g.vertices = make(map[string]*Vertex)
	g.aggregators = make(map[string]Aggregator)
	return nil
}

func (g *Graph) Close() error {
	close(g.vertexCh)
	g.wg.Wait()
	return nil
}

func (g *Graph) step() (int, error) {
	var err error

	g.activeInStep, g.pendingInStep = 0, int64(len(g.vertices))

	if g.pendingInStep == 0 {
		return 0, nil
	}

	for _, v := range g.vertices {
		g.vertexCh <- v
	}

	<-g.stepCompletedCh

	select {
	case err = <-g.errCh:
	default:
	}

	return int(g.activeInStep), err
}

func (g *Graph) startWorkers(numWorkers int) {
	g.vertexCh = make(chan *Vertex)
	g.errCh = make(chan error, 1)
	g.stepCompletedCh = make(chan struct{})

	g.wg.Add(numWorkers)
	for range numWorkers {
		go g.stepWorker()
	}
}

func (g *Graph) stepWorker() {
	for v := range g.vertexCh {
		buffer := g.superstep % 2
		if v.active || v.msgQueue[buffer].PendingMessages() {
			_ = atomic.AddInt64(&g.activeInStep, 1)
			v.active = true
			if err := g.computeFn(g, v, v.msgQueue[buffer].Messages()); err != nil {
				tryEmitError(g.errCh, fmt.Errorf("running compute function for vertex %q failed: %w", v.ID(), err))
			} else if err := v.msgQueue[buffer].DiscardMessages(); err != nil {
				tryEmitError(g.errCh, fmt.Errorf("discarding unprocessed messages for vertex %q failed: %w", v.ID(), err))
			}
		}
		if atomic.AddInt64(&g.pendingInStep, -1) == 0 {
			g.stepCompletedCh <- struct{}{}
		}
	}
	g.wg.Done()
}

func tryEmitError(errCh chan<- error, err error) {
	select {
	case errCh <- err: // queued error
	default: // channel already contains another error
	}
}

package bspgraph

import "context"

type Executor struct {
	g  *Graph
	cb ExecutorCallbacks
}

type ExecutorCallbacks struct {
	PreStep             func(ctx context.Context, g *Graph) error
	PostStep            func(ctx context.Context, g *Graph, activeInStep int) error
	PostStepKeepRunning func(ctx context.Context, g *Graph, activeInStep int) (bool, error)
}

func NewExecutor(g *Graph, cb ExecutorCallbacks) *Executor {
	patchEmptyCallbacks(&cb)
	g.superstep = 0
	return &Executor{
		g:  g,
		cb: cb,
	}
}

func (ex *Executor) Graph() *Graph { return ex.g }

func (ex *Executor) Superstep() int { return ex.g.Superstep() }

func (ex *Executor) RunSteps(ctx context.Context, numSteps int) error { return ex.run(ctx, numSteps) }

func (ex *Executor) RunToCompletion(ctx context.Context) error { return ex.run(ctx, -1) }

func (ex *Executor) run(ctx context.Context, maxSteps int) error {
	var (
		activeInStep int
		err          error
		keepRunning  bool
		cb           = ex.cb
	)

	for ; maxSteps != 0; ex.g.superstep, maxSteps = ex.g.superstep+1, maxSteps-1 {
		if err = ensureContextNotExpired(ctx); err != nil {
			break
		} else if err = cb.PreStep(ctx, ex.g); err != nil {
			break
		} else if activeInStep, err = ex.g.step(); err != nil {
			break
		} else if err = cb.PostStep(ctx, ex.g, activeInStep); err != nil {
			break
		} else if keepRunning, err = cb.PostStepKeepRunning(ctx, ex.g, activeInStep); !keepRunning || err != nil {
			break
		}
	}

	return err
}

func patchEmptyCallbacks(cb *ExecutorCallbacks) {
	if cb.PreStep == nil {
		cb.PreStep = func(context.Context, *Graph) error { return nil }
	}
	if cb.PostStep == nil {
		cb.PostStep = func(context.Context, *Graph, int) error { return nil }
	}
	if cb.PostStepKeepRunning == nil {
		cb.PostStepKeepRunning = func(context.Context, *Graph, int) (bool, error) { return true, nil }
	}
}

func ensureContextNotExpired(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

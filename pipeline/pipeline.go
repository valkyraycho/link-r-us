package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
)

var _ StageParams = (*workerParams)(nil)

type workerParams struct {
	stage int

	inCh  <-chan Payload
	outCh chan<- Payload
	errCh chan<- error
}

func (p *workerParams) StageIndex() int { return p.stage }

func (p *workerParams) Input() <-chan Payload  { return p.inCh }
func (p *workerParams) Output() chan<- Payload { return p.outCh }
func (p *workerParams) Error() chan<- error    { return p.errCh }

// Pipeline implements a modular, multi-stage pipeline. Each pipeline is
// constructed out of an input source, an output sink and zero or more
// processing stages.
type Pipeline struct {
	stages []StageRunner
}

func New(stages ...StageRunner) *Pipeline {
	return &Pipeline{
		stages: stages,
	}
}

func (p *Pipeline) Process(ctx context.Context, source Source, sink Sink) error {
	var wg sync.WaitGroup
	pctx, cancel := context.WithCancel(ctx)

	stageCh := make([]chan Payload, len(p.stages)+1)
	errCh := make(chan error, len(p.stages)+2)
	for i := range len(stageCh) {
		stageCh[i] = make(chan Payload)
	}

	for i := range p.stages {
		wg.Add(1)
		go func(stageIndex int) {
			defer wg.Done()
			p.stages[stageIndex].Run(pctx, &workerParams{
				stage: stageIndex,
				inCh:  stageCh[stageIndex],
				outCh: stageCh[stageIndex+1],
				errCh: errCh,
			})
			close(stageCh[stageIndex+1])
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		sourceWorker(ctx, source, stageCh[0], errCh)
		close(stageCh[0])
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		sinkWorker(ctx, sink, stageCh[len(stageCh)-1], errCh)
	}()

	go func() {
		wg.Wait()
		close(errCh)
		cancel()
	}()

	var err error
	for pErr := range errCh {
		err = multierror.Append(err, pErr)
		cancel()
	}
	return err
}

// sourceWorker implements a worker that reads Payload instances from a Source
// and pushes them to an output channel that is used as input for the first
// stage of the pipeline.
func sourceWorker(ctx context.Context, source Source, outCh chan<- Payload, errCh chan<- error) {
	for source.Next(ctx) {
		payload := source.Payload()
		select {
		case outCh <- payload:
		case <-ctx.Done():
			return
		}
	}

	if err := source.Error(); err != nil {
		wrappedErr := fmt.Errorf("pipeline source: %w", err)
		maybeEmitError(wrappedErr, errCh)
	}
}

// sinkWorker implements a worker that reads Payload instances from an input
// channel (the output of the last pipeline stage) and passes them to the
// provided sink.
func sinkWorker(ctx context.Context, sink Sink, inCh <-chan Payload, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-inCh:
			if !ok {
				return
			}

			if err := sink.Consume(ctx, payload); err != nil {
				wrappedErr := fmt.Errorf("pipeline sink: %w", err)
				maybeEmitError(wrappedErr, errCh)
				return
			}
			payload.MarkAsProcessed()
		}
	}
}

func maybeEmitError(err error, errCh chan<- error) {
	select {
	case errCh <- err:
	default:
	}
}

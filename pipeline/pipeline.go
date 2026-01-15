package pipeline

import (
	"context"
	"fmt"
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

func maybeEmitError(err error, errCh chan<- error) {
	select {
	case errCh <- err:
	default:
	}
}

package pipeline

import (
	"context"
	"fmt"
	"sync"
)

type fifo struct {
	proc Processor
}

// FIFO returns a StageRunner that processes incoming payloads in a first-in
// first-out fashion. Each input is passed to the specified processor and its
// output is emitted to the next stage.
func FIFO(proc Processor) StageRunner {
	return &fifo{proc: proc}
}

func (r *fifo) Run(ctx context.Context, params StageParams) {
	for {
		select {
		case <-ctx.Done():
			return
		case payloadIn, ok := <-params.Input():
			if !ok {
				return
			}
			payloadOut, err := r.proc.Process(ctx, payloadIn)
			if err != nil {
				wrappedErr := fmt.Errorf("pipeline stage %d: %w", params.StageIndex(), err)
				maybeEmitError(wrappedErr, params.Error())
				return
			}

			if payloadOut == nil {
				payloadIn.MarkAsProcessed()
				continue
			}
			select {
			case <-ctx.Done():
				return
			case params.Output() <- payloadOut:
			}
		}
	}
}

type fixedWorkerPool struct {
	fifos []StageRunner
}

// FixedWorkerPool returns a StageRunner that spins up a pool containing
// numWorkers to process incoming payloads in parallel and emit their outputs
// to the next stage.
func FixedWorkerPool(proc Processor, numWorkers int) StageRunner {
	if numWorkers <= 0 {
		panic("FixedWorkerPool: numWorkers must be > 0")
	}
	fifos := make([]StageRunner, numWorkers)
	for i := range numWorkers {
		fifos[i] = FIFO(proc)
	}
	return &fixedWorkerPool{fifos: fifos}
}

func (p *fixedWorkerPool) Run(ctx context.Context, params StageParams) {
	var wg sync.WaitGroup

	for i := range len(p.fifos) {
		wg.Add(1)
		go func(fifoIndex int) {
			p.fifos[fifoIndex].Run(ctx, params)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

type dynamicWorkerPool struct {
	proc      Processor
	tokenPool chan struct{}
}

// DynamicWorkerPool returns a StageRunner that maintains a dynamic worker pool
// that can scale up to maxWorkers for processing incoming inputs in parallel
// and emitting their outputs to the next stage.
func DynamicWorkerPool(proc Processor, maxWorkers int) StageRunner {
	if maxWorkers <= 0 {
		panic("DynamicWorkerPool: maxWorkers must be > 0")
	}

	tokenPool := make(chan struct{}, maxWorkers)
	for range maxWorkers {
		tokenPool <- struct{}{}
	}
	return &dynamicWorkerPool{proc: proc, tokenPool: tokenPool}
}

func (p *dynamicWorkerPool) Run(ctx context.Context, params StageParams) {
stop:
	for {
		select {
		case <-ctx.Done():
			break stop
		case payloadIn, ok := <-params.Input():
			if !ok {
				break stop
			}
			var token struct{}
			select {
			case token = <-p.tokenPool:
			case <-ctx.Done():
				break stop
			}

			go func(payloadIn Payload, token struct{}) {
				defer func() { p.tokenPool <- token }()
				payloadOut, err := p.proc.Process(ctx, payloadIn)
				if err != nil {
					wrappedErr := fmt.Errorf("pipeline stage %d: %w", params.StageIndex(), err)
					maybeEmitError(wrappedErr, params.Error())
					return
				}

				if payloadOut == nil {
					payloadIn.MarkAsProcessed()
					return
				}

				select {
				case params.Output() <- payloadOut:
				case <-ctx.Done():
				}
			}(payloadIn, token)
		}
	}

	for range cap(p.tokenPool) {
		<-p.tokenPool
	}
}

type broadcast struct {
	fifos []StageRunner
}

func Broadbast(procs ...Processor) StageRunner {
	if len(procs) == 0 {
		panic("Broadcast: at least one processor must be specified")
	}

	fifos := make([]StageRunner, len(procs))
	for i, p := range procs {
		fifos[i] = FIFO(p)
	}
	return &broadcast{fifos: fifos}
}

func (b *broadcast) Run(ctx context.Context, params StageParams) {
	var (
		wg    sync.WaitGroup
		inChs = make([]chan Payload, len(b.fifos))
	)

	for i := range len(b.fifos) {
		wg.Add(1)
		inChs[i] = make(chan Payload)
		go func(fifoIndex int) {
			fifoParams := &workerParams{
				stage: params.StageIndex(),
				inCh:  inChs[i],
				outCh: params.Output(),
				errCh: params.Error(),
			}
			b.fifos[fifoIndex].Run(ctx, fifoParams)
			wg.Done()
		}(i)
	}

done:
	for {
		select {
		case <-ctx.Done():
			break done
		case payload, ok := <-params.Input():
			if !ok {
				break done
			}
			for i := range len(inChs) {
				var fifoPayload = payload
				if i != 0 {
					fifoPayload = payload.Clone()
				}
				select {
				case <-ctx.Done():
					break done
				case inChs[i] <- fifoPayload:
				}
			}
		}
	}

	for _, inCh := range inChs {
		close(inCh)
	}
	wg.Wait()
}

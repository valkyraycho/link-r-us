package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/valkyraycho/links-r-us/pipeline"

	"gopkg.in/check.v1"
)

var _ = check.Suite(new(PipelineTestSuite))

func Test(t *testing.T) { check.TestingT(t) }

type PipelineTestSuite struct{}

func (s *PipelineTestSuite) TestDataFlow(c *check.C) {
	stages := make([]pipeline.StageRunner, 10)
	for i := range stages {
		stages[i] = testStage{c: c}
	}

	src := &sourceStub{data: stringPayloads(3)}
	sink := new(sinkStub)

	p := pipeline.New(stages...)
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, check.IsNil)
	c.Assert(sink.data, check.DeepEquals, src.data)
	assertAllProcessed(c, src.data)
}

func (s *PipelineTestSuite) TestProcessorErrorHandling(c *check.C) {
	expErr := errors.New("some error")
	stages := make([]pipeline.StageRunner, 10)
	for i := 0; i < len(stages); i++ {
		var err error
		if i == 5 {
			err = expErr
		}

		stages[i] = testStage{c: c, err: err}
	}

	src := &sourceStub{data: stringPayloads(3)}
	sink := new(sinkStub)

	p := pipeline.New(stages...)
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, check.ErrorMatches, "(?s).*some error.*")
}

func (s *PipelineTestSuite) TestSourceErrorHandling(c *check.C) {
	expErr := errors.New("some error")
	src := &sourceStub{data: stringPayloads(3), err: expErr}
	sink := new(sinkStub)

	p := pipeline.New(testStage{c: c})
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, check.ErrorMatches, "(?s).*pipeline source: some error.*")
}

func (s *PipelineTestSuite) TestSinkErrorHandling(c *check.C) {
	expErr := errors.New("some error")
	src := &sourceStub{data: stringPayloads(3)}
	sink := &sinkStub{err: expErr}

	p := pipeline.New(testStage{c: c})
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, check.ErrorMatches, "(?s).*pipeline sink: some error.*")
}

func (s *PipelineTestSuite) TestPayloadDiscarding(c *check.C) {
	src := &sourceStub{data: stringPayloads(3)}
	sink := &sinkStub{}

	p := pipeline.New(testStage{c: c, dropPayloads: true})
	err := p.Process(context.TODO(), src, sink)
	c.Assert(err, check.IsNil)
	c.Assert(sink.data, check.HasLen, 0, check.Commentf("expected all payloads to be discarded by stage processor"))
	assertAllProcessed(c, src.data)
}

func assertAllProcessed(c *check.C, payloads []pipeline.Payload) {
	for i, p := range payloads {
		payload := p.(*stringPayload)
		c.Assert(payload.processed, check.Equals, true, check.Commentf("payload %d not processed", i))
	}
}

type testStage struct {
	c            *check.C
	dropPayloads bool
	err          error
}

func (s testStage) Run(ctx context.Context, params pipeline.StageParams) {
	defer func() {
		s.c.Logf("[stage %d] exiting", params.StageIndex())
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-params.Input():
			if !ok {
				return
			}
			s.c.Logf("[stage %d] received payload: %v", params.StageIndex(), p)
			if s.err != nil {
				s.c.Logf("[stage %d] emit error: %v", params.StageIndex(), s.err)
				params.Error() <- s.err
				return
			}

			if s.dropPayloads {
				s.c.Logf("[stage %d] dropping payload: %v", params.StageIndex(), p)
				p.MarkAsProcessed()
				continue
			}

			s.c.Logf("[stage %d] emitting payload: %v", params.StageIndex(), p)
			select {
			case <-ctx.Done():
				return
			case params.Output() <- p:
			}
		}
	}
}

type sourceStub struct {
	index int
	data  []pipeline.Payload
	err   error
}

func (s *sourceStub) Next(context.Context) bool {
	if s.err != nil || s.index == len(s.data) {
		return false
	}

	s.index++
	return true
}
func (s *sourceStub) Error() error { return s.err }
func (s *sourceStub) Payload() pipeline.Payload {
	return s.data[s.index-1]
}

type sinkStub struct {
	data []pipeline.Payload
	err  error
}

func (s *sinkStub) Consume(_ context.Context, p pipeline.Payload) error {
	s.data = append(s.data, p)
	return s.err
}

type stringPayload struct {
	processed bool
	val       string
}

func (s *stringPayload) Clone() pipeline.Payload { return &stringPayload{val: s.val} }
func (s *stringPayload) MarkAsProcessed()        { s.processed = true }
func (s *stringPayload) String() string          { return s.val }

func stringPayloads(numValues int) []pipeline.Payload {
	out := make([]pipeline.Payload, numValues)
	for i := range out {
		out[i] = &stringPayload{val: fmt.Sprint(i)}
	}
	return out
}

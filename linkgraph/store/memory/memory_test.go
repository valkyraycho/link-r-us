package memory

import (
	"testing"

	"github.com/valkyraycho/links-r-us/linkgraph/graph/graphtest"
	"gopkg.in/check.v1"
)

var _ = check.Suite(new(InMemoryGraphTestSuite))

type InMemoryGraphTestSuite struct {
	graphtest.SuiteBase
}

func (s *InMemoryGraphTestSuite) SetUpTest(c *check.C) {
	s.SetGraph(NewInMemoryGraph())
}

func Test(t *testing.T) {
	check.TestingT(t)
}

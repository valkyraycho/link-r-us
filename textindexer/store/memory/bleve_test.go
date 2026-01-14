package memory

import (
	"testing"

	"github.com/valkyraycho/links-r-us/textindexer/index/indextest"
	"gopkg.in/check.v1"
)

var _ = check.Suite(new(InMemoryBleveIndexerTestSuite))

type InMemoryBleveIndexerTestSuite struct {
	indextest.SuiteBase
	idx *InMemoryBleveIndexer
}

func Test(t *testing.T) {
	check.TestingT(t)
}

func (s *InMemoryBleveIndexerTestSuite) SetUpTest(c *check.C) {
	idx, err := NewInMemoryBleveIndexer()
	c.Assert(err, check.IsNil)
	s.SetIndexer(idx)
	s.idx = idx
}

func (s *InMemoryBleveIndexerTestSuite) TearDownTest(c *check.C) {
	c.Assert(s.idx.Close(), check.IsNil)
}

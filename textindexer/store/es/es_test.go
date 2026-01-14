package es

import (
	"os"
	"strings"
	"testing"

	"github.com/valkyraycho/links-r-us/textindexer/index/indextest"
	"gopkg.in/check.v1"
)

var _ = check.Suite(new(ElasticSearchTestSuite))

type ElasticSearchTestSuite struct {
	indextest.SuiteBase
	idx *ElasticSearchIndexer
}

func Test(t *testing.T) {
	check.TestingT(t)
}

func (s *ElasticSearchTestSuite) SetUpSuite(c *check.C) {
	nodeList := os.Getenv("ES_NODES")
	if nodeList == "" {
		c.Skip("Missing ES_NODES envvar; skipping elasticsearch-backed index test suite")
	}

	idx, err := NewElasticSearchIndexer(strings.Split(nodeList, ","), true)
	c.Assert(err, check.IsNil)
	s.SetIndexer(idx)
	s.idx = idx
}

func (s *ElasticSearchTestSuite) SetUpTest(c *check.C) {
	if s.idx.es == nil {
		return
	}

	_, err := s.idx.es.Indices.Delete([]string{indexName})
	c.Assert(err, check.IsNil)
	err = ensureIndex(s.idx.es)
	c.Assert(err, check.IsNil)
}

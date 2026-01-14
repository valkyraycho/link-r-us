package cdb

import (
	"database/sql"
	"os"
	"testing"

	"github.com/valkyraycho/links-r-us/linkgraph/graph/graphtest"
	"gopkg.in/check.v1"
)

var _ = check.Suite(new(CockroachDBGraphTestSuite))

func Test(t *testing.T) {
	check.TestingT(t)
}

type CockroachDBGraphTestSuite struct {
	graphtest.SuiteBase
	db *sql.DB
}

func (s *CockroachDBGraphTestSuite) SetUpSuite(c *check.C) {
	dsn := os.Getenv("CDB_DSN")
	if dsn == "" {
		c.Skip("Missing CDB_DSN envvar; skipping cockroachdb-backed graph test suite")
	}

	g, err := NewCockroachDBGraph(dsn)
	c.Assert(err, check.IsNil)
	s.SetGraph(g)

	s.db = g.db
}

func (s *CockroachDBGraphTestSuite) SetUpTest(c *check.C) {
	s.flushDB(c)
}

func (s *CockroachDBGraphTestSuite) flushDB(c *check.C) {
	_, err := s.db.Exec("DELETE FROM links")
	c.Assert(err, check.IsNil)
	_, err = s.db.Exec("DELETE FROM edges")
	c.Assert(err, check.IsNil)
}

func (s *CockroachDBGraphTestSuite) TearDownSuite(c *check.C) {
	if s.db != nil {
		s.flushDB(c)
		c.Assert(s.db.Close(), check.IsNil)
	}
}

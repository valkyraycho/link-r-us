package graphtest

import (
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/valkyraycho/links-r-us/linkgraph/graph"

	"gopkg.in/check.v1"
)

type SuiteBase struct {
	g graph.Graph
}

func (s *SuiteBase) SetGraph(g graph.Graph) {
	s.g = g
}

func (s *SuiteBase) TestUpsertLink(c *check.C) {
	original := &graph.Link{
		RetrievedAt: time.Now().Add(-10 * time.Hour),
	}
	err := s.g.UpsertLink(original)
	c.Assert(err, check.IsNil)
	c.Assert(original.ID, check.Not(check.Equals), uuid.Nil, check.Commentf("expected a linkID to be assigned to the new link"))

	accessedAt := time.Now().Truncate(time.Second).UTC()
	existing := &graph.Link{
		ID:          original.ID,
		RetrievedAt: accessedAt,
	}
	err = s.g.UpsertLink(existing)
	c.Assert(err, check.IsNil)
	c.Assert(existing.ID, check.Equals, original.ID, check.Commentf("link ID changed while upserting"))

	stored, err := s.g.FindLink(existing.ID)
	c.Assert(err, check.IsNil)
	c.Assert(stored.RetrievedAt, check.Equals, accessedAt, check.Commentf("last accessed timestamp was not updated"))

	sameURL := &graph.Link{
		URL:         existing.URL,
		RetrievedAt: time.Now().Add(-10 * time.Hour).UTC(),
	}
	err = s.g.UpsertLink(sameURL)
	c.Assert(err, check.IsNil)
	c.Assert(sameURL.ID, check.Equals, existing.ID)

	stored, err = s.g.FindLink(existing.ID)
	c.Assert(err, check.IsNil)
	c.Assert(stored.RetrievedAt, check.Equals, accessedAt, check.Commentf("last accessed timestamp was overwritten with an older value"))

	dup := &graph.Link{
		URL: "foo",
	}
	err = s.g.UpsertLink(dup)
	c.Assert(err, check.IsNil)
	c.Assert(dup.ID, check.Not(check.Equals), uuid.Nil, check.Commentf("expected a linkID to be assigned to the new link"))
}

func (s *SuiteBase) TestFindLink(c *check.C) {
	link := &graph.Link{
		URL:         "https://example.com",
		RetrievedAt: time.Now().Truncate(time.Second).UTC(),
	}

	err := s.g.UpsertLink(link)
	c.Assert(err, check.IsNil)
	c.Assert(link.ID, check.Not(check.Equals), uuid.Nil, check.Commentf("expected a linkID to be assigned to the new link"))

	other, err := s.g.FindLink(link.ID)
	c.Assert(err, check.IsNil)
	c.Assert(other, check.DeepEquals, link, check.Commentf("lookup by ID returned the wrong link"))

	_, err = s.g.FindLink(uuid.Nil)
	c.Assert(errors.Is(err, graph.ErrNotFound), check.Equals, true)
}

func (s *SuiteBase) TestConcurrentLinkIterators(c *check.C) {
	var (
		wg           sync.WaitGroup
		numIterators = 10
		numLinks     = 100
	)

	for i := range numLinks {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), check.IsNil)
	}

	wg.Add(numIterators)
	for i := range numIterators {
		go func(id int) {
			defer wg.Done()

			itTacheckomment := check.Commentf("iterator %d", id)
			seen := make(map[string]bool)
			it, err := s.partitionedLinkIterator(c, 0, 1, time.Now())
			c.Assert(err, check.IsNil, itTacheckomment)
			defer func() {
				c.Assert(it.Close(), check.IsNil, itTacheckomment)
			}()

			for i := 0; it.Next(); i++ {
				link := it.Link()
				linkID := link.ID.String()
				c.Assert(seen[linkID], check.Equals, false, check.Commentf("iterator %d saw same link twice", id))
				seen[linkID] = true
			}

			c.Assert(seen, check.HasLen, numLinks, itTacheckomment)
			c.Assert(it.Error(), check.IsNil, itTacheckomment)
			c.Assert(it.Close(), check.IsNil, itTacheckomment)
		}(i)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		c.Fatal("timed out waiting for test to complete")
	}
}

func (s *SuiteBase) TestLinkIteratorTimeFilter(c *check.C) {
	linkUUIDs := make([]uuid.UUID, 3)
	linkInsertTimes := make([]time.Time, len(linkUUIDs))
	for i := 0; i < len(linkUUIDs); i++ {
		link := &graph.Link{URL: fmt.Sprint(i), RetrievedAt: time.Now()}
		c.Assert(s.g.UpsertLink(link), check.IsNil)
		linkUUIDs[i] = link.ID
		linkInsertTimes[i] = time.Now()
	}

	for i, t := range linkInsertTimes {
		c.Logf("fetching links created before edge %d", i)
		s.assertIteratedLinkIDsMatch(c, t, linkUUIDs[:i+1])
	}
}

func (s *SuiteBase) assertIteratedLinkIDsMatch(c *check.C, updatedBefore time.Time, exp []uuid.UUID) {
	it, err := s.partitionedLinkIterator(c, 0, 1, updatedBefore)
	c.Assert(err, check.IsNil)

	var got []uuid.UUID
	for it.Next() {
		got = append(got, it.Link().ID)
	}
	c.Assert(it.Error(), check.IsNil)
	c.Assert(it.Close(), check.IsNil)

	sort.Slice(got, func(l, r int) bool { return got[l].String() < got[r].String() })
	sort.Slice(exp, func(l, r int) bool { return exp[l].String() < exp[r].String() })
	c.Assert(got, check.DeepEquals, exp)
}

func (s *SuiteBase) TestPartitionedLinkIterators(c *check.C) {
	numLinks := 100
	numPartitions := 10
	for i := range numLinks {
		c.Assert(s.g.UpsertLink(&graph.Link{URL: fmt.Sprint(i)}), check.IsNil)
	}

	c.Assert(s.iteratePartitionedLinks(c, numPartitions), check.Equals, numLinks)
	c.Assert(s.iteratePartitionedLinks(c, numPartitions+1), check.Equals, numLinks)
}

func (s *SuiteBase) iteratePartitionedLinks(c *check.C, numPartitions int) int {
	seen := make(map[string]bool)
	for partition := range numPartitions {
		it, err := s.partitionedLinkIterator(c, partition, numPartitions, time.Now())
		c.Assert(err, check.IsNil)
		defer func() {
			c.Assert(it.Close(), check.IsNil)
		}()

		for it.Next() {
			link := it.Link()
			linkID := link.ID.String()
			c.Assert(seen[linkID], check.Equals, false, check.Commentf("iterator returned same link in different partitions"))
			seen[linkID] = true
		}

		c.Assert(it.Error(), check.IsNil)
		c.Assert(it.Close(), check.IsNil)
	}

	return len(seen)
}

func (s *SuiteBase) TestUpsertEdge(c *check.C) {
	linkUUIDs := make([]uuid.UUID, 3)
	for i := range 3 {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), check.IsNil)
		linkUUIDs[i] = link.ID
	}

	edge := &graph.Edge{
		Src: linkUUIDs[0],
		Dst: linkUUIDs[1],
	}

	err := s.g.UpsertEdge(edge)
	c.Assert(err, check.IsNil)
	c.Assert(edge.ID, check.Not(check.Equals), uuid.Nil, check.Commentf("expected an edgeID to be assigned to the new edge"))
	c.Assert(edge.UpdatedAt.IsZero(), check.Equals, false, check.Commentf("UpdatedAt field not set"))

	other := &graph.Edge{
		ID:  edge.ID,
		Src: linkUUIDs[0],
		Dst: linkUUIDs[1],
	}
	err = s.g.UpsertEdge(other)
	c.Assert(err, check.IsNil)
	c.Assert(other.ID, check.Equals, edge.ID, check.Commentf("edge ID changed while upserting"))
	c.Assert(other.UpdatedAt, check.Not(check.Equals), edge.UpdatedAt, check.Commentf("UpdatedAt field not modified"))

	bogus := &graph.Edge{
		Src: linkUUIDs[0],
		Dst: uuid.New(),
	}
	err = s.g.UpsertEdge(bogus)
	c.Assert(errors.Is(err, graph.ErrUnknownEdgeLinks), check.Equals, true)
}

func (s *SuiteBase) TestConcurrentEdgeIterators(c *check.C) {
	var (
		wg           sync.WaitGroup
		numIterators = 10
		numEdges     = 100
		linkUUIDs    = make([]uuid.UUID, numEdges*2)
	)

	for i := 0; i < numEdges*2; i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), check.IsNil)
		linkUUIDs[i] = link.ID
	}
	for i := 0; i < numEdges; i++ {
		c.Assert(s.g.UpsertEdge(&graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[i],
		}), check.IsNil)
	}

	wg.Add(numIterators)
	for i := 0; i < numIterators; i++ {
		go func(id int) {
			defer wg.Done()

			itTacheckomment := check.Commentf("iterator %d", id)
			seen := make(map[string]bool)
			it, err := s.partitionedEdgeIterator(c, 0, 1, time.Now())
			c.Assert(err, check.IsNil, itTacheckomment)
			defer func() {
				c.Assert(it.Close(), check.IsNil, itTacheckomment)
			}()

			for i := 0; it.Next(); i++ {
				edge := it.Edge()
				edgeID := edge.ID.String()
				c.Assert(seen[edgeID], check.Equals, false, check.Commentf("iterator %d saw same edge twice", id))
				seen[edgeID] = true
			}

			c.Assert(seen, check.HasLen, numEdges, itTacheckomment)
			c.Assert(it.Error(), check.IsNil, itTacheckomment)
			c.Assert(it.Close(), check.IsNil, itTacheckomment)
		}(i)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		c.Fatal("timed out waiting for test to complete")
	}
}

func (s *SuiteBase) TestEdgeIteratorTimeFilter(c *check.C) {
	linkUUIDs := make([]uuid.UUID, 3)
	linkInsertTimes := make([]time.Time, len(linkUUIDs))
	for i := 0; i < len(linkUUIDs); i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), check.IsNil)
		linkUUIDs[i] = link.ID
		linkInsertTimes[i] = time.Now()
	}

	edgeUUIDs := make([]uuid.UUID, len(linkUUIDs))
	edgeInsertTimes := make([]time.Time, len(linkUUIDs))
	for i := 0; i < len(linkUUIDs); i++ {
		edge := &graph.Edge{Src: linkUUIDs[0], Dst: linkUUIDs[i]}
		c.Assert(s.g.UpsertEdge(edge), check.IsNil)
		edgeUUIDs[i] = edge.ID
		edgeInsertTimes[i] = time.Now()
	}

	for i, t := range edgeInsertTimes {
		c.Logf("fetching edges created before edge %d", i)
		s.assertIteratedEdgeIDsMatch(c, t, edgeUUIDs[:i+1])
	}
}

func (s *SuiteBase) assertIteratedEdgeIDsMatch(c *check.C, updatedBefore time.Time, exp []uuid.UUID) {
	it, err := s.partitionedEdgeIterator(c, 0, 1, updatedBefore)
	c.Assert(err, check.IsNil)

	var got []uuid.UUID
	for it.Next() {
		got = append(got, it.Edge().ID)
	}
	c.Assert(it.Error(), check.IsNil)
	c.Assert(it.Close(), check.IsNil)

	sort.Slice(got, func(l, r int) bool { return got[l].String() < got[r].String() })
	sort.Slice(exp, func(l, r int) bool { return exp[l].String() < exp[r].String() })
	c.Assert(got, check.DeepEquals, exp)
}

func (s *SuiteBase) TestPartitionedEdgeIterators(c *check.C) {
	numEdges := 100
	numPartitions := 10
	linkUUIDs := make([]uuid.UUID, numEdges*2)
	for i := 0; i < numEdges*2; i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), check.IsNil)
		linkUUIDs[i] = link.ID
	}
	for i := 0; i < numEdges; i++ {
		c.Assert(s.g.UpsertEdge(&graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[i],
		}), check.IsNil)
	}

	c.Assert(s.iteratePartitionedEdges(c, numPartitions), check.Equals, numEdges)
	c.Assert(s.iteratePartitionedEdges(c, numPartitions+1), check.Equals, numEdges)
}

func (s *SuiteBase) iteratePartitionedEdges(c *check.C, numPartitions int) int {
	seen := make(map[string]bool)
	for partition := 0; partition < numPartitions; partition++ {
		linksInPartition := make(map[uuid.UUID]struct{})
		linkIt, err := s.partitionedLinkIterator(c, partition, numPartitions, time.Now())
		c.Assert(err, check.IsNil)
		for linkIt.Next() {
			linkID := linkIt.Link().ID
			linksInPartition[linkID] = struct{}{}
		}

		it, err := s.partitionedEdgeIterator(c, partition, numPartitions, time.Now())
		c.Assert(err, check.IsNil)
		defer func() {
			c.Assert(it.Close(), check.IsNil)
		}()

		for it.Next() {
			edge := it.Edge()
			edgeID := edge.ID.String()
			c.Assert(seen[edgeID], check.Equals, false, check.Commentf("iterator returned same edge in different partitions"))
			seen[edgeID] = true

			_, srcInPartition := linksInPartition[edge.Src]
			c.Assert(srcInPartition, check.Equals, true, check.Commentf("iterator returned an edge whose source link belongs to a different partition"))
		}

		c.Assert(it.Error(), check.IsNil)
		c.Assert(it.Close(), check.IsNil)
	}

	return len(seen)
}

func (s *SuiteBase) TestRemoveStaleEdges(c *check.C) {
	numEdges := 100
	linkUUIDs := make([]uuid.UUID, numEdges*4)
	goneUUIDs := make(map[uuid.UUID]struct{})
	for i := 0; i < numEdges*4; i++ {
		link := &graph.Link{URL: fmt.Sprint(i)}
		c.Assert(s.g.UpsertLink(link), check.IsNil)
		linkUUIDs[i] = link.ID
	}

	var lastTs time.Time
	for i := 0; i < numEdges; i++ {
		e1 := &graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[i],
		}
		c.Assert(s.g.UpsertEdge(e1), check.IsNil)
		goneUUIDs[e1.ID] = struct{}{}
		lastTs = e1.UpdatedAt
	}

	deleteBefore := lastTs.Add(time.Millisecond)
	time.Sleep(250 * time.Millisecond)

	for i := 0; i < numEdges; i++ {
		e2 := &graph.Edge{
			Src: linkUUIDs[0],
			Dst: linkUUIDs[numEdges+i+1],
		}
		c.Assert(s.g.UpsertEdge(e2), check.IsNil)
	}
	c.Assert(s.g.RemoveStaleEdges(linkUUIDs[0], deleteBefore), check.IsNil)

	it, err := s.partitionedEdgeIterator(c, 0, 1, time.Now())
	c.Assert(err, check.IsNil)
	defer func() { c.Assert(it.Close(), check.IsNil) }()

	var seen int
	for it.Next() {
		id := it.Edge().ID
		_, found := goneUUIDs[id]
		c.Assert(found, check.Equals, false, check.Commentf("expected edge %s to be removed from the edge list", id.String()))
		seen++
	}

	c.Assert(seen, check.Equals, numEdges)
}

func (s *SuiteBase) partitionedLinkIterator(c *check.C, partition, numPartitions int, accessedBefore time.Time) (graph.LinkIterator, error) {
	from, to := s.partitionRange(c, partition, numPartitions)
	return s.g.Links(from, to, accessedBefore)
}

func (s *SuiteBase) partitionedEdgeIterator(c *check.C, partition, numPartitions int, updatedBefore time.Time) (graph.EdgeIterator, error) {
	from, to := s.partitionRange(c, partition, numPartitions)
	return s.g.Edges(from, to, updatedBefore)
}

func (s *SuiteBase) partitionRange(c *check.C, partition, numPartitions int) (from, to uuid.UUID) {
	if partition < 0 || partition >= numPartitions {
		c.Fatal("invalid partition")
	}

	var minUUID = uuid.Nil
	var maxUUID = uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	var err error

	tokenRange := big.NewInt(0)
	partSize := big.NewInt(0)
	partSize.SetBytes(maxUUID[:])
	partSize = partSize.Div(partSize, big.NewInt(int64(numPartitions)))

	if partition == 0 {
		from = minUUID
	} else {
		tokenRange.Mul(partSize, big.NewInt(int64(partition)))
		from, err = uuid.FromBytes(tokenRange.Bytes())
		c.Assert(err, check.IsNil)
	}

	if partition == numPartitions-1 {
		to = maxUUID
	} else {
		tokenRange.Mul(partSize, big.NewInt(int64(partition+1)))
		to, err = uuid.FromBytes(tokenRange.Bytes())
		c.Assert(err, check.IsNil)
	}

	return from, to
}

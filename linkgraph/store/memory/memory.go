package memory

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/valkyraycho/links-r-us/linkgraph/graph"
)

// edgeList contains the slice of edge UUIDs that originate from a link in the graph.
type edgeList []uuid.UUID

// InMemoryGraph implements an in-memory link graph that can be concurrently
// accessed by multiple clients.
type InMemoryGraph struct {
	mu sync.RWMutex

	links map[uuid.UUID]*graph.Link
	edges map[uuid.UUID]*graph.Edge

	linkURLIndex map[string]*graph.Link
	linkEdgeMap  map[uuid.UUID]edgeList
}

// NewInMemoryGraph creates a new in-memory link graph.
func NewInMemoryGraph() *InMemoryGraph {
	return &InMemoryGraph{
		links:        make(map[uuid.UUID]*graph.Link),
		edges:        make(map[uuid.UUID]*graph.Edge),
		linkURLIndex: make(map[string]*graph.Link),
		linkEdgeMap:  make(map[uuid.UUID]edgeList),
	}
}

func (s *InMemoryGraph) UpsertLink(link *graph.Link) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing := s.linkURLIndex[link.URL]; existing != nil {
		link.ID = existing.ID
		origTs := existing.RetrievedAt
		*existing = *link
		if origTs.After(existing.RetrievedAt) {
			existing.RetrievedAt = origTs
		}
		return nil
	}

	for {
		link.ID = uuid.New()
		if s.links[link.ID] == nil {
			break
		}
	}

	lCopy := new(graph.Link)
	*lCopy = *link
	s.linkURLIndex[link.URL] = lCopy
	s.links[link.ID] = lCopy
	return nil
}

func (s *InMemoryGraph) FindLink(id uuid.UUID) (*graph.Link, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	link := s.links[id]
	if link == nil {
		return nil, fmt.Errorf("find link: %w", graph.ErrNotFound)
	}

	lCopy := new(graph.Link)
	*lCopy = *link
	return lCopy, nil
}

func (s *InMemoryGraph) Links(fromID, toID uuid.UUID, retrievedBefore time.Time) (graph.LinkIterator, error) {
	from, to := fromID.String(), toID.String()

	s.mu.RLock()
	var links []*graph.Link
	for linkID, link := range s.links {
		if id := linkID.String(); id >= from && id < to && link.RetrievedAt.Before(retrievedBefore) {
			links = append(links, link)
		}
	}
	s.mu.RUnlock()
	return &linkIterator{s: s, links: links}, nil
}

func (s *InMemoryGraph) UpsertEdge(edge *graph.Edge) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, srcExists := s.links[edge.Src]
	_, dstExists := s.links[edge.Dst]
	if !srcExists || !dstExists {
		return fmt.Errorf("upsert edge: %w", graph.ErrUnknownEdgeLinks)
	}

	for _, edgeID := range s.linkEdgeMap[edge.Src] {
		existingEdge := s.edges[edgeID]
		if existingEdge.Src == edge.Src && existingEdge.Dst == edge.Dst {
			existingEdge.UpdatedAt = time.Now()
			*edge = *existingEdge
			return nil
		}
	}

	for {
		edge.ID = uuid.New()
		if s.edges[edge.ID] == nil {
			break
		}
	}
	edge.UpdatedAt = time.Now()
	eCopy := new(graph.Edge)
	*eCopy = *edge
	s.edges[edge.ID] = eCopy
	s.linkEdgeMap[edge.Src] = append(s.linkEdgeMap[edge.Src], edge.ID)
	return nil
}

func (s *InMemoryGraph) Edges(fromID, toID uuid.UUID, updatedBefore time.Time) (graph.EdgeIterator, error) {
	from, to := fromID.String(), toID.String()
	var edges []*graph.Edge

	s.mu.RLock()

	for linkID := range s.links {
		if id := linkID.String(); id >= from && id < to {
			for _, edgeID := range s.linkEdgeMap[linkID] {
				edge := s.edges[edgeID]
				if edge.UpdatedAt.Before(updatedBefore) {
					edges = append(edges, edge)
				}
			}
		}
	}
	return &edgeIterator{s: s, edges: edges}, nil
}

func (s *InMemoryGraph) RemoveStaleEdges(fromID uuid.UUID, updatedBefore time.Time) error {
	var newEdgeList edgeList

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, edgeID := range s.linkEdgeMap[fromID] {
		edge := s.edges[edgeID]
		if edge.UpdatedAt.After(updatedBefore) {
			newEdgeList = append(newEdgeList, edgeID)
		} else {
			delete(s.edges, edgeID)
		}
	}

	s.linkEdgeMap[fromID] = newEdgeList
	return nil
}

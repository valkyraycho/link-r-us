package memory

import "github.com/valkyraycho/links-r-us/linkgraph/graph"

type linkIterator struct {
	s        *InMemoryGraph
	links    []*graph.Link
	curIndex int
}

func (i *linkIterator) Next() bool {
	if i.curIndex >= len(i.links) {
		return false
	}
	i.curIndex++
	return true
}

func (i *linkIterator) Error() error {
	return nil
}

func (i *linkIterator) Close() error {
	return nil
}

func (i *linkIterator) Link() *graph.Link {
	i.s.mu.RLock()
	defer i.s.mu.RUnlock()

	link := new(graph.Link)
	*link = *i.links[i.curIndex-1]
	return link
}

type edgeIterator struct {
	s        *InMemoryGraph
	edges    []*graph.Edge
	curIndex int
}

func (i *edgeIterator) Next() bool {
	if i.curIndex >= len(i.edges) {
		return false
	}
	i.curIndex++
	return true
}

func (i *edgeIterator) Error() error {
	return nil
}

func (i *edgeIterator) Close() error {
	return nil
}

func (i *edgeIterator) Edge() *graph.Edge {
	i.s.mu.RLock()
	edge := new(graph.Edge)
	*edge = *i.edges[i.curIndex-1]
	i.s.mu.RUnlock()
	return edge
}

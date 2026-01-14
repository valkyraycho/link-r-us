package memory

import (
	"github.com/blevesearch/bleve/v2"
	"github.com/valkyraycho/links-r-us/textindexer/index"
)

type bleveIterator struct {
	idx       *InMemoryBleveIndexer
	searchReq *bleve.SearchRequest

	cumIdx uint64
	rsIdx  int
	rs     *bleve.SearchResult

	latchedDoc *index.Document
	lastErr    error
}

func (it *bleveIterator) Close() error {
	it.idx = nil
	it.searchReq = nil
	if it.rs != nil {
		it.cumIdx = it.rs.Total
	}
	return nil
}

func (it *bleveIterator) Next() bool {
	if it.lastErr != nil || it.rs == nil || it.cumIdx >= it.rs.Total {
		return false
	}

	if it.rsIdx >= it.rs.Hits.Len() {
		it.searchReq.From += it.searchReq.Size
		if it.rs, it.lastErr = it.idx.idx.Search(it.searchReq); it.lastErr != nil {
			return false
		}
		it.rsIdx = 0
	}

	nextID := it.rs.Hits[it.rsIdx].ID
	if it.latchedDoc, it.lastErr = it.idx.findByID(nextID); it.lastErr != nil {
		return false
	}

	it.rsIdx++
	it.cumIdx++
	return true
}

func (it *bleveIterator) Error() error {
	return it.lastErr
}

func (it *bleveIterator) Document() *index.Document {
	return it.latchedDoc
}

func (it *bleveIterator) TotalCount() uint64 {
	if it.rs == nil {
		return 0
	}
	return it.rs.Total
}

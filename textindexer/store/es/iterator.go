package es

import (
	"github.com/elastic/go-elasticsearch/v9"
	"github.com/valkyraycho/links-r-us/textindexer/index"
)

type esIterator struct {
	es        *elasticsearch.Client
	searchReq map[string]any

	cumIdx uint64
	rsIdx  int
	rs     *esSearchRes

	latchedDoc *index.Document
	lastErr    error
}

func (it *esIterator) Close() error {
	it.es = nil
	it.searchReq = nil
	it.cumIdx = it.rs.Hits.Total.Count
	return nil
}

func (it *esIterator) Next() bool {
	if it.lastErr != nil || it.rs == nil || it.cumIdx >= it.rs.Hits.Total.Count {
		return false
	}

	if it.rsIdx >= len(it.rs.Hits.HitList) {
		it.searchReq["from"] = it.searchReq["from"].(uint64) + batchSize
		if it.rs, it.lastErr = runSearch(it.es, it.searchReq); it.lastErr != nil {
			return false
		}
		it.rsIdx = 0
	}

	it.latchedDoc = mapEsDoc(&it.rs.Hits.HitList[it.rsIdx].DocSource)
	it.cumIdx++
	it.rsIdx++
	return true
}

func (it *esIterator) Error() error {
	return it.lastErr
}

func (it *esIterator) Document() *index.Document {
	return it.latchedDoc
}

func (it *esIterator) TotalCount() uint64 {
	return it.rs.Hits.Total.Count
}

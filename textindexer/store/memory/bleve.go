package memory

import (
	"fmt"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/google/uuid"
	"github.com/valkyraycho/links-r-us/textindexer/index"
)

const batchSize = 10

var _ index.Indexer = (*InMemoryBleveIndexer)(nil)

type bleveDoc struct {
	Title    string
	Content  string
	PageRank float64
}

// InMemoryBleveIndexer is an Indexer implementation that uses an in-memory
// bleve instance to catalogue and search documents.
type InMemoryBleveIndexer struct {
	mu   sync.RWMutex
	docs map[string]*index.Document

	idx bleve.Index
}

func NewInMemoryBleveIndexer() (*InMemoryBleveIndexer, error) {
	mapping := bleve.NewIndexMapping()
	ids, err := bleve.NewMemOnly(mapping)
	if err != nil {
		return nil, err
	}

	return &InMemoryBleveIndexer{
		idx:  ids,
		docs: make(map[string]*index.Document),
	}, nil
}

func (i *InMemoryBleveIndexer) Close() error {
	return i.idx.Close()
}

func (i *InMemoryBleveIndexer) Index(doc *index.Document) error {
	if doc.LinkID == uuid.Nil {
		return fmt.Errorf("index: %w", index.ErrMissingLinkID)
	}

	doc.IndexedAt = time.Now()
	dcopy := copyDoc(doc)

	key := dcopy.LinkID.String()

	i.mu.Lock()
	if orig, exists := i.docs[key]; exists {
		dcopy.PageRank = orig.PageRank
	}

	if err := i.idx.Index(key, makeBleveDoc(dcopy)); err != nil {
		return fmt.Errorf("index: %w", err)
	}

	i.docs[key] = dcopy
	defer i.mu.Unlock()
	return nil
}

func copyDoc(d *index.Document) *index.Document {
	dcopy := new(index.Document)
	*dcopy = *d
	return dcopy
}

func makeBleveDoc(d *index.Document) bleveDoc {
	return bleveDoc{
		Title:    d.Title,
		Content:  d.Content,
		PageRank: d.PageRank,
	}
}

func (i *InMemoryBleveIndexer) FindByID(linkID uuid.UUID) (*index.Document, error) {
	return i.findByID(linkID.String())
}

func (i *InMemoryBleveIndexer) findByID(linkID string) (*index.Document, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if d, found := i.docs[linkID]; found {
		return copyDoc(d), nil
	}

	return nil, fmt.Errorf("find by ID: %w", index.ErrNotFound)
}

func (i *InMemoryBleveIndexer) UpdateScore(linkID uuid.UUID, score float64) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	key := linkID.String()
	doc, found := i.docs[key]
	if !found {
		doc = &index.Document{LinkID: linkID}
		i.docs[key] = doc
	}

	doc.PageRank = score
	if err := i.idx.Index(key, makeBleveDoc(doc)); err != nil {
		return fmt.Errorf("update score: %w", err)
	}
	return nil
}

func (i *InMemoryBleveIndexer) Search(q index.Query) (index.Iterator, error) {
	var bq query.Query

	switch q.Type {
	case index.QueryTypePhrase:
		bq = bleve.NewMatchPhraseQuery(q.Expression)
	default:
		bq = bleve.NewMatchQuery(q.Expression)
	}

	searchReq := bleve.NewSearchRequest(bq)
	searchReq.SortBy([]string{"-PageRank", "-_score"})
	searchReq.Size = batchSize
	searchReq.From = int(q.Offset)
	rs, err := i.idx.Search(searchReq)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	return &bleveIterator{idx: i, searchReq: searchReq, cumIdx: q.Offset, rs: rs}, nil
}

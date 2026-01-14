package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
	"github.com/google/uuid"
	"github.com/valkyraycho/links-r-us/textindexer/index"
)

const indexName = "textindexer"
const batchSize = 10

type esError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func (e esError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Reason)
}

type esUpdateRes struct {
	Result string `json:"result"`
}
type esErrorRes struct {
	Error esError `json:"error"`
}

var esMappings = `
{
  "mappings" : {
    "properties": {
      "LinkID": {"type": "keyword"},
      "URL": {"type": "keyword"},
      "Content": {"type": "text"},
      "Title": {"type": "text"},
      "IndexedAt": {"type": "date"},
      "PageRank": {"type": "double"}
    }
  }
}`

type esSearchRes struct {
	Hits esSearchResHits `json:"hits"`
}

type esSearchResHits struct {
	Total   esTotal        `json:"total"`
	HitList []esHitWrapper `json:"hits"`
}

type esTotal struct {
	Count uint64 `json:"value"`
}

type esHitWrapper struct {
	DocSource esDoc `json:"_source"`
}

type esDoc struct {
	LinkID    string    `json:"LinkID"`
	URL       string    `json:"URL"`
	Title     string    `json:"Title"`
	Content   string    `json:"Content"`
	IndexedAt time.Time `json:"IndexedAt"`
	PageRank  float64   `json:"PageRank,omitempty"`
}

var _ index.Indexer = (*ElasticSearchIndexer)(nil)

// ElasticSearchIndexer is an Indexer implementation that uses an elastic search
// instance to catalogue and search documents.
type ElasticSearchIndexer struct {
	es         *elasticsearch.Client
	refreshOpt func(*esapi.UpdateRequest)
}

func NewElasticSearchIndexer(esNodes []string, syncUpdates bool) (*ElasticSearchIndexer, error) {
	cfg := elasticsearch.Config{
		Addresses: esNodes,
		Username:  os.Getenv("ES_USERNAME"),
		Password:  os.Getenv("ES_PASSWORD"),
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	if err := ensureIndex(es); err != nil {
		return nil, err
	}

	refreshOpt := es.Update.WithRefresh("false")
	if syncUpdates {
		refreshOpt = es.Update.WithRefresh("true")
	}

	return &ElasticSearchIndexer{
		es:         es,
		refreshOpt: refreshOpt,
	}, nil
}

func (i *ElasticSearchIndexer) Index(doc *index.Document) error {
	if doc.LinkID == uuid.Nil {
		return fmt.Errorf("index: %w", index.ErrMissingLinkID)
	}

	var (
		buf   bytes.Buffer
		esDoc = makeEsDoc(doc)
	)
	update := map[string]any{
		"doc":           esDoc,
		"doc_as_upsert": true,
	}

	if err := json.NewEncoder(&buf).Encode(update); err != nil {
		return fmt.Errorf("index: %w", err)
	}

	res, err := i.es.Update(indexName, esDoc.LinkID, &buf, i.refreshOpt)
	if err != nil {
		return fmt.Errorf("index: %w", err)
	}

	var updateRes esUpdateRes
	if res.IsError() {
		return unmarshalError(res)
	}
	if err := unmarshalResponse(res, &updateRes); err != nil {
		return fmt.Errorf("index: %w", err)
	}
	return nil
}

func (i *ElasticSearchIndexer) FindByID(linkID uuid.UUID) (*index.Document, error) {
	var buf bytes.Buffer

	query := map[string]any{
		"query": map[string]any{
			"match": map[string]any{
				"LinkID": linkID.String(),
			},
		},
		"from": 0,
		"size": 1,
	}

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("find by ID: %w", err)
	}

	searchRes, err := runSearch(i.es, query)
	if err != nil {
		return nil, fmt.Errorf("find by ID: %w", err)
	}
	if len(searchRes.Hits.HitList) != 1 {
		return nil, fmt.Errorf("find by ID: %w", index.ErrNotFound)
	}

	return mapEsDoc(&searchRes.Hits.HitList[0].DocSource), nil
}

func (i *ElasticSearchIndexer) Search(q index.Query) (index.Iterator, error) {
	var qtype string
	switch q.Type {
	case index.QueryTypePhrase:
		qtype = "phrase"
	default:
		qtype = "best_fields"
	}
	query := map[string]any{
		"query": map[string]any{
			"function_score": map[string]any{
				"query": map[string]any{
					"multi_match": map[string]any{
						"type":   qtype,
						"query":  q.Expression,
						"fields": []string{"Title", "Content"},
					},
				},
				"script_score": map[string]any{
					"script": map[string]any{
						"source": "_score + doc['PageRank'].value",
					},
				},
			},
		},
		"from": q.Offset,
		"size": batchSize,
	}
	searchRes, err := runSearch(i.es, query)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}
	return &esIterator{es: i.es, searchReq: query, cumIdx: q.Offset, rs: searchRes}, nil
}

func (i *ElasticSearchIndexer) UpdateScore(linkID uuid.UUID, score float64) error {
	var buf bytes.Buffer

	update := map[string]any{
		"doc": map[string]any{
			"LinkID":   linkID.String(),
			"PageRank": score,
		},
		"doc_as_upsert": true,
	}

	if err := json.NewEncoder(&buf).Encode(update); err != nil {
		return fmt.Errorf("update score: %w", err)
	}

	res, err := i.es.Update(indexName, linkID.String(), &buf, i.refreshOpt)
	if err != nil {
		return fmt.Errorf("update score: %w", err)
	}

	var updateRes esUpdateRes
	if res.IsError() {
		return unmarshalError(res)
	}
	if err := unmarshalResponse(res, &updateRes); err != nil {
		return fmt.Errorf("update score: %w", err)
	}
	return nil
}

func ensureIndex(es *elasticsearch.Client) error {
	mappingsReader := strings.NewReader(esMappings)
	res, err := es.Indices.Create(indexName, es.Indices.Create.WithBody(mappingsReader))
	if err != nil {
		return fmt.Errorf("cannot create ES index: %w", err)
	}

	if res.IsError() {
		err := unmarshalError(res)
		if esErr, valid := err.(esError); valid && esErr.Type == "resource_already_exists_exception" {
			return nil
		}

		return fmt.Errorf("cannot create ES index: %w", err)
	}
	return nil
}

func runSearch(es *elasticsearch.Client, searchQuery map[string]any) (*esSearchRes, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(searchQuery); err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(indexName),
		es.Search.WithBody(&buf),
	)

	if err != nil {
		return nil, err
	}

	var searchRes esSearchRes
	if err := unmarshalResponse(res, &searchRes); err != nil {
		return nil, err
	}
	return &searchRes, nil
}

func unmarshalError(res *esapi.Response) error {
	return unmarshalResponse(res, nil)
}

func unmarshalResponse(res *esapi.Response, to any) error {
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		var errRes esErrorRes
		if err := json.NewDecoder(res.Body).Decode(&errRes); err != nil {
			return err
		}
		return errRes.Error
	}
	return json.NewDecoder(res.Body).Decode(to)
}

func mapEsDoc(d *esDoc) *index.Document {
	return &index.Document{
		LinkID:    uuid.MustParse(d.LinkID),
		URL:       d.URL,
		Title:     d.Title,
		Content:   d.Content,
		IndexedAt: d.IndexedAt.UTC(),
		PageRank:  d.PageRank,
	}
}

func makeEsDoc(d *index.Document) esDoc {
	return esDoc{
		LinkID:    d.LinkID.String(),
		URL:       d.URL,
		Title:     d.Title,
		Content:   d.Content,
		IndexedAt: d.IndexedAt,
	}
}

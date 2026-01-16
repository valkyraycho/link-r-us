package crawler

import (
	"context"
	"net/http"

	"github.com/valkyraycho/links-r-us/linkgraph/graph"
	"github.com/valkyraycho/links-r-us/pipeline"
)

var _ pipeline.Source = (*linkSource)(nil)

type linkSource struct {
	linkIt graph.LinkIterator
}

func (ls *linkSource) Error() error {
	return ls.linkIt.Error()
}

func (ls *linkSource) Next(context.Context) bool {
	return ls.linkIt.Next()
}

func (ls *linkSource) Payload() pipeline.Payload {
	link := ls.linkIt.Link()
	p := payloadPool.Get().(*crawlerPayload)
	p.LinkID = link.ID
	p.URL = link.URL
	p.RetrievedAt = link.RetrievedAt
	return p
}

type nopSink struct{}

func (nopSink) Consume(context.Context, pipeline.Payload) error { return nil }

// URLGetter is implemented by objects that can perform HTTP GET requests.
type URLGetter interface {
	Get(url string) (*http.Response, error)
}

// PrivateNetworkDetector is implemented by objects that can detect whether a
// host resolves to a private network address.
type PrivateNetworkDetector interface {
	IsPrivate(host string) (bool, error)
}

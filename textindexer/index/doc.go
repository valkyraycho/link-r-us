package index

import (
	"time"

	"github.com/google/uuid"
)

type Document struct {
	LinkID uuid.UUID

	URL string

	Title   string
	Content string

	IndexedAt time.Time
	PageRank  float64
}

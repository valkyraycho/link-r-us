package message

import "sync"

type inMemoryQueue struct {
	mu   sync.Mutex
	msgs []Message

	latchedMsg Message
}

func NewInMemoryQueue() Queue {
	return new(inMemoryQueue)
}

func (q *inMemoryQueue) Enqueue(msg Message) error {
	q.mu.Lock()
	q.msgs = append(q.msgs, msg)
	q.mu.Unlock()
	return nil
}

func (q *inMemoryQueue) PendingMessages() bool {
	q.mu.Lock()
	pending := len(q.msgs) != 0
	q.mu.Unlock()
	return pending
}

func (q *inMemoryQueue) DiscardMessages() error {
	q.mu.Lock()
	q.msgs = q.msgs[:0]
	q.mu.Unlock()
	return nil
}

func (*inMemoryQueue) Close() error { return nil }

func (*inMemoryQueue) Error() error { return nil }

func (q *inMemoryQueue) Next() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	qLen := len(q.msgs)
	if qLen == 0 {
		return false
	}

	q.latchedMsg = q.msgs[qLen-1]
	q.msgs = q.msgs[:qLen-1]
	return true
}

func (q *inMemoryQueue) Message() Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	msg := q.latchedMsg
	return msg
}

func (q *inMemoryQueue) Messages() Iterator { return q }

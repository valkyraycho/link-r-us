package message

// Message is implemented by types that can be processed by a Queue.
type Message interface {
	Type() string
}

type Queue interface {
	Close() error

	Enqueue(msg Message) error

	PendingMessages() bool

	DiscardMessages() error

	Messages() Iterator
}

type Iterator interface {
	Next() bool

	Message() Message

	Error() error
}

type QueueFactory func() Queue

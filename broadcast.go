package gossip

import (
	"errors"
	"sync"
)

var ErrBroadcasterClosed = errors.New("broadcaster is already closed and not accepting new subscribers or messages")

type Broadcaster[T any] interface {
	Subscribe() (chan T, error)
	Unsubscribe(chan<- T)
	Broadcast(T) error
	Close()
}

type DefaultBroadcaster[T any] struct {
	subscribers map[chan<- T]struct{}
	subStream   chan chan<- T
	unSubStream chan chan<- T
	pubStream   chan T
	done        chan struct{}
	doneAck     chan struct{}
	closed      bool
	closeOnce   sync.Once
	mu          sync.RWMutex
}

// NewBroadcaster returns a new instance of the default Broadcaster implementation with the given input buffer length.
func NewBroadcaster[T any](bufLen int) *DefaultBroadcaster[T] {
	b := &DefaultBroadcaster[T]{
		subscribers: make(map[chan<- T]struct{}),
		subStream:   make(chan chan<- T),
		unSubStream: make(chan chan<- T),
		pubStream:   make(chan T, bufLen),
	}

	go b.broadcast()

	return b
}

/*
Subscribe adds a new subscriber to the broadcaster. All subscribers will receive messages published to the broadcaster.
The returned channel shouldn't be closed by the client to prevent panics. Instead, the client should call Unsubscribe() on that channel to remove the subscriber from the broadcaster.
*/
func (b *DefaultBroadcaster[T]) Subscribe() (chan T, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrBroadcasterClosed
	}
	sub := make(chan T)
	b.subStream <- sub
	return sub, nil
}

// Unsubscribe removes a subscriber (that is, the channel returned by Subscribe()) from the broadcaster so that it no longer receives messages.
func (b *DefaultBroadcaster[T]) Unsubscribe(sub chan<- T) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	b.unSubStream <- sub
}

// Broadcast sends a message to all subscribers of the broadcaster.
func (b *DefaultBroadcaster[T]) Broadcast(msg T) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrBroadcasterClosed
	}
	b.pubStream <- msg
	return nil
}

// Close closes the broadcaster and all the channels previously returned by Subscribe().
func (b *DefaultBroadcaster[T]) Close() {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		close(b.done)
		<-b.doneAck

		close(b.subStream)
		close(b.pubStream)
		close(b.unSubStream)
		for sub := range b.subscribers {
			b.removeSubscriber(sub)
		}
		b.closed = true
	})
}

func (b *DefaultBroadcaster[T]) broadcast() {
	for {
		select {
		case <-b.done:
			close(b.doneAck)
			return
		case sub := <-b.subStream:
			b.subscribers[sub] = struct{}{}
		case sub := <-b.unSubStream:
			b.removeSubscriber(sub)
		case msg := <-b.pubStream:
			for sub := range b.subscribers {
				sub <- msg
			}
		}
	}
}

func (b *DefaultBroadcaster[T]) removeSubscriber(sub chan<- T) {
	delete(b.subscribers, sub)
	close(sub)
}

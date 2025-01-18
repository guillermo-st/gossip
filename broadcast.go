package gossip

import (
	"errors"
	"time"
)

var ErrBroadcasterClosed = errors.New("broadcaster is already closed and not accepting new subscribers or messages")

type Broadcaster[T any] interface {
	Subscribe() (Subscription[T], error)
	Broadcast(T) error
	Close()
}

type DefaultBroadcaster[T any] struct {
	subscribers    map[chan<- T]struct{}
	subStream      chan chan<- T
	unSubStream    chan chan T
	pubStream      chan T
	done           chan struct{}
	doneAck        chan struct{}
	subSendTimeout time.Duration
	subBufLen      int
}

// NewBroadcaster returns a new instance of the default Broadcaster implementation with the given input buffer length, subscriber (output) buffer length, and a timeout for slow subscribers.
func NewBroadcaster[T any](broadcastBufLen, subBufLen int, subSendTimeout time.Duration) *DefaultBroadcaster[T] {
	b := &DefaultBroadcaster[T]{
		subscribers:    make(map[chan<- T]struct{}),
		subStream:      make(chan chan<- T),
		unSubStream:    make(chan chan T),
		pubStream:      make(chan T, broadcastBufLen),
		done:           make(chan struct{}),
		doneAck:        make(chan struct{}),
		subBufLen:      subBufLen,
		subSendTimeout: subSendTimeout,
	}

	go b.broadcast()

	return b
}

// Subscribe adds a new subscriber to the broadcaster and returns the corresponding Subscription to read from. All subscribers will receive messages published to the broadcaster.
func (b *DefaultBroadcaster[T]) Subscribe() (Subscription[T], error) {
	sub := make(chan T, b.subBufLen)
	select {
	case b.subStream <- sub:
		return &defaultSubscription[T]{
			channel: sub,
			close: func() {
				b.unsubscribe(sub)
			},
		}, nil
	case <-b.done:
		close(sub)
		return nil, ErrBroadcasterClosed
	}
}

// Broadcast sends a message to all subscribers of the broadcaster.
func (b *DefaultBroadcaster[T]) Broadcast(msg T) error {
	select {
	case b.pubStream <- msg:
		return nil
	case <-b.done:
		return ErrBroadcasterClosed
	}
}

// Close closes the broadcaster and all the channels previously returned by Subscribe().
func (b *DefaultBroadcaster[T]) Close() {
	select {
	case <-b.done:
		return
	default:
		close(b.done)
	}

	<-b.doneAck

	close(b.subStream)
	close(b.unSubStream)
	close(b.pubStream)
}

// unsubscribe removes a subscriber (that is, the channel returned by Subscribe()) from the broadcaster so that it no longer receives messages.
func (b *DefaultBroadcaster[T]) unsubscribe(sub chan T) {
	select {
	case b.unSubStream <- sub:
	case <-b.done:
	}
}

func (b *DefaultBroadcaster[T]) broadcast() {
	for {
		select {
		case <-b.done:
			for sub := range b.subscribers {
				b.removeSubscriber(sub)
			}

			close(b.doneAck)
			return

		case sub := <-b.subStream:
			b.subscribers[sub] = struct{}{}

		case sub := <-b.unSubStream:
			b.removeSubscriber(sub)

		case msg := <-b.pubStream:
			for sub := range b.subscribers {
				select {
				case sub <- msg:
				case <-time.After(b.subSendTimeout):
					continue
				}
			}
		}
	}
}

func (b *DefaultBroadcaster[T]) removeSubscriber(sub chan<- T) {
	delete(b.subscribers, sub)
	close(sub)
}

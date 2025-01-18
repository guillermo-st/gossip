package gossip

import "sync"

// Subscription represents a subscription to a broadcaster. It provides a channel to read the broadcasted messages from and a method to close the subscription.
type Subscription[T any] interface {
	Channel() <-chan T
	Close()
}

type defaultSubscription[T any] struct {
	channel <-chan T
	close   func()
	once    sync.Once
}

func (s *defaultSubscription[T]) Channel() <-chan T {
	return s.channel
}

func (s *defaultSubscription[T]) Close() {
	s.once.Do(s.close)
}

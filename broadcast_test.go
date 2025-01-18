package gossip

import (
	"testing"
	"time"
)

func TestBroadcaster_RaceCondition(t *testing.T) {
	// Create a broadcaster with a buffer of 1
	b := NewBroadcaster[int](0, 0, 2*time.Second)
	const numSubscribers = 5

	// Create multiple subscribers
	subs := make([]Subscription[int], numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		sub, err := b.Subscribe()
		if err != nil {
			t.Fatalf("unexpected error subscribing: %v", err)
		}
		subs[i] = sub
	}

	// Broadcast a message
	go func() {
		for i := 0; i < 10; i++ {
			err := b.Broadcast(i)
			if err != nil {
				t.Errorf("unexpected error broadcasting: %v", err)
			}
		}
		b.Close()
	}()

	// Read from all subscribers in parallel
	for i := 0; i < numSubscribers; i++ {
		go func(i int) {
			for msg := range subs[i].Channel() {
				t.Logf("Subscriber %d received: %d", i, msg)
			}
		}(i)
	}

	// Unsubscribe all subscribers
	for _, sub := range subs {
		sub.Close()
	}

}

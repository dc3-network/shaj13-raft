package msgbus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMsgBus(t *testing.T) {
	eventid := uint64(1)
	onceid := uint64(2)

	val := "some value"
	m := New()
	s1 := m.Subscribe(eventid)
	s2 := m.SubscribeOnce(onceid)
	defer m.Close()

	run := func(id uint64, s1, s2 *Subscription) bool {
		go func() {
			m.Broadcast(id, val)
		}()

		select {
		case v, ok := <-s1.Chan():
			if ok {
				assert.Equal(t, val, v)
				return true
			}
			return false
		case <-s2.Chan():
			t.Fatal("unexpected broadcast call")
		case <-time.After(time.Millisecond * 500):
			return false
		}

		return false
	}

	unsubscribe := func(s *Subscription) {
		s.Unsubscribe()
		m.mu.Lock()
		defer m.mu.Unlock()
		assert.Equal(t, len(m.events[s.eid]), 0)
		assert.Equal(t, len(m.events), 1)
	}

	// Round #1 check normal subscribe
	// caled more than once.
	for i := 0; i < 2; i++ {
		ok := run(eventid, s1, s2)
		assert.True(t, ok, "subscribe chan not called")
	}

	unsubscribe(s1)

	// Round #2 check once subscribe
	// caled only once.
	s1 = m.Subscribe(eventid)
	count := 0
	for i := 0; i < 2; i++ {
		if ok := run(onceid, s2, s1); ok {
			count++
		}
	}

	assert.Equalf(
		t,
		1,
		count,
		"subscribe once chan not called or called mor than once, Count %d",
		count,
	)

	unsubscribe(s2)
}

// A once subscription leaves the bus with the broadcast that delivers it, so a
// second broadcast cannot deliver it again. It used to stay until its publish
// goroutine unsubscribed it, and TestMsgBus saw two deliveries.
func TestSubscribeOnceLeavesWithTheFirstBroadcast(t *testing.T) {
	m := New()
	defer m.Close()

	s := m.SubscribeOnce(1)
	other := m.Subscribe(1)
	m.Broadcast(1, "first")

	m.mu.Lock()
	_, onceLeft := m.events[1][s.id]
	_, otherLeft := m.events[1][other.id]
	m.mu.Unlock()
	assert.False(t, onceLeft, "once subscription still subscribed after a broadcast")
	assert.True(t, otherLeft, "regular subscription dropped by a broadcast")

	// one goroutine publishes to every subscriber in turn: drain each value
	// before the next broadcast, or a full channel blocks the others.
	assert.Equal(t, "first", <-s.Chan())
	assert.Equal(t, "first", <-other.Chan())
	m.Broadcast(1, "second")
	assert.Equal(t, "second", <-other.Chan())
	<-s.closed // its publish has unsubscribed it
	select {
	case v := <-s.Chan():
		t.Fatalf("once subscription delivered twice, second value %v", v)
	default:
	}
}

// A subscription can be deleted twice: a once subscription's publish
// unsubscribes it, then its owner's deferred Unsubscribe runs (the engine does
// both). The second delete must not drop another subscription to the same
// event, here a LinearizableRead waiting for the same index.
func TestUnsubscribeTwiceKeepsOtherSubscriptions(t *testing.T) {
	m := New()
	defer m.Close()

	first := m.SubscribeOnce(1)
	first.Unsubscribe()
	second := m.SubscribeOnce(1)
	first.Unsubscribe()

	m.Broadcast(1, "value")
	select {
	case v := <-second.Chan():
		assert.Equal(t, "value", v)
	case <-time.After(5 * time.Second):
		t.Fatal("subscription dropped by another subscription's second Unsubscribe")
	}
}

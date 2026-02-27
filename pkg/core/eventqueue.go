package core

import "container/heap"

type eventHeap []*Event

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)        { *h = append(*h, x.(*Event)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type EventQueue struct {
	h *eventHeap
}

func NewEventQueue() *EventQueue {
	h := &eventHeap{}
	heap.Init(h)
	return &EventQueue{h: h}
}

func (q *EventQueue) Push(e *Event) { heap.Push(q.h, e) }

func (q *EventQueue) Pop() *Event {
	if q.h.Len() == 0 {
		return nil
	}
	return heap.Pop(q.h).(*Event)
}

// Peek returns the next event without removing it from the queue.
// Returns nil if the queue is empty.
func (q *EventQueue) Peek() *Event {
	if q.h.Len() == 0 {
		return nil
	}
	return (*q.h)[0]
}

func (q *EventQueue) Len() int { return q.h.Len() }

package core

import (
	"math"
	"sync"
)

// unboundedInbox is a thread-safe, unbounded queue of events.
// Senders never block — they append to the queue under a lock.
// The receiver drains the entire queue at once during barrier phase 1.
type unboundedInbox struct {
	mu    sync.Mutex
	items []*Event
}

func newUnboundedInbox() *unboundedInbox {
	return &unboundedInbox{}
}

// send appends an event to the queue. Never blocks.
func (u *unboundedInbox) send(ev *Event) {
	u.mu.Lock()
	u.items = append(u.items, ev)
	u.mu.Unlock()
}

// drainAll removes and returns all queued events. The caller owns the slice.
func (u *unboundedInbox) drainAll() []*Event {
	u.mu.Lock()
	items := u.items
	u.items = nil
	u.mu.Unlock()
	return items
}

// LogicalProcess represents a single LP in the CMB parallel simulation.
// Each LP runs on its own goroutine, owns a local event queue, and
// communicates with other LPs exclusively through unbounded inbox queues.
// Synchronization with the coordinator happens via barrier channels.
type LogicalProcess struct {
	id        NodeID
	node      *Node
	clock     int64
	algo      Algorithm
	delay     DelayModel
	topo      TopologyReader
	log       *Logger
	failed    map[[2]NodeID]bool
	lookahead map[NodeID]int64

	// localQueue holds events owned by this LP: timers, start events,
	// crash/recover events, and messages drained from inboxes.
	localQueue *EventQueue

	// inbox: one unbounded queue per incoming neighbor.
	// Other LPs deposit messages here; this LP drains them at the
	// start of each barrier cycle.
	inbox map[NodeID]*unboundedInbox

	// outbox: references to the inbox queues of neighboring LPs.
	// This LP sends generated messages into these queues (never blocks).
	outbox map[NodeID]*unboundedInbox

	// Barrier synchronization with the coordinator.
	reportCh chan<- lpReport // LP → coordinator: local LBTS contribution
	lbtsCh   <-chan int64    // coordinator → LP: computed global LBTS
	doneCh   chan<- struct{} // LP → coordinator: cycle processing complete

	// stepCount tracks how many events this LP processed (for global total).
	stepCount int
}

// lpReport is sent by each LP to the coordinator at the start of every
// barrier cycle. It carries the LP's contribution to the LBTS computation.
type lpReport struct {
	nodeID   NodeID
	value    int64 // min(next event timestamp) + min(lookahead), or maxInt64 if idle
	finished bool  // true when LP has no events and inbox is empty
}

// newLogicalProcess creates a new LP for the given node.
func newLogicalProcess(
	id NodeID,
	node *Node,
	algo Algorithm,
	delay DelayModel,
	topo TopologyReader,
	log *Logger,
	lookahead map[NodeID]int64,
) *LogicalProcess {
	return &LogicalProcess{
		id:         id,
		node:       node,
		algo:       algo,
		delay:      delay,
		topo:       topo,
		log:        log,
		failed:     make(map[[2]NodeID]bool),
		lookahead:  lookahead,
		localQueue: NewEventQueue(),
		inbox:      make(map[NodeID]*unboundedInbox),
		outbox:     make(map[NodeID]*unboundedInbox),
	}
}

// run is the main loop executed by each LP goroutine.
// It follows the CMB barrier-synchronization protocol:
//
//  1. Drain inbox — collect messages deposited by other LPs in the previous cycle.
//  2. Report — send local LBTS contribution (Ni + LAi) to the coordinator.
//  3. Wait — receive the global LBTS from the coordinator.
//  4. Process — execute all local events with timestamp ≤ LBTS.
//  5. Signal — notify the coordinator that this cycle is complete.
//
// The loop terminates when the coordinator broadcasts a termination signal
// (LBTS == math.MaxInt64), meaning all LPs have no more events.
func (lp *LogicalProcess) run(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		// ── Phase 1: drain inbox ──────────────────────────────────
		// Read all messages deposited by other LPs during the
		// previous cycle and insert them into the local queue.
		lp.drainInbox()

		// ── Phase 2: report to coordinator ────────────────────────
		// Compute this LP's contribution to the global LBTS.
		// If the LP has pending events, the contribution is
		// Ni + min_lookahead, where Ni is the timestamp of the
		// next event. If the LP is idle, it reports maxInt64.
		ni := lp.nextEventTime()
		finished := ni == math.MaxInt64
		value := int64(math.MaxInt64)
		if !finished {
			value = ni + lp.minLookahead()
		}
		lp.reportCh <- lpReport{
			nodeID:   lp.id,
			value:    value,
			finished: finished,
		}

		// ── Phase 3: receive global LBTS ──────────────────────────
		lbts := <-lp.lbtsCh
		if lbts == math.MaxInt64 {
			// Termination signal: all LPs are idle.
			return
		}

		// ── Phase 4: process safe events ──────────────────────────
		// Process all events whose timestamp is ≤ LBTS. These are
		// guaranteed to be safe: no future message from any LP can
		// have a timestamp ≤ LBTS because delay ≥ lookahead > 0.
		for lp.localQueue.Len() > 0 && lp.localQueue.Peek().Time <= lbts {
			ev := lp.localQueue.Pop()
			lp.clock = ev.Time
			lp.stepCount++
			lp.processEvent(ev)
		}

		// ── Phase 5: signal cycle complete ────────────────────────
		lp.doneCh <- struct{}{}
	}
}

// drainInbox reads all available messages from every inbox channel
// (non-blocking) and pushes them into the local event queue.
func (lp *LogicalProcess) drainInbox() {
	for _, ub := range lp.inbox {
		for _, ev := range ub.drainAll() {
			lp.localQueue.Push(ev)
		}
	}
}

// nextEventTime returns the timestamp of the next event in the local queue,
// or math.MaxInt64 if the queue is empty.
func (lp *LogicalProcess) nextEventTime() int64 {
	if lp.localQueue.Len() == 0 {
		return math.MaxInt64
	}
	return lp.localQueue.Peek().Time
}

// minLookahead returns the minimum lookahead across all outgoing links
// of this LP. This is the tightest guarantee this LP can make about the
// timestamp of its future messages.
func (lp *LogicalProcess) minLookahead() int64 {
	minLA := int64(math.MaxInt64)
	for _, la := range lp.lookahead {
		if la < minLA {
			minLA = la
		}
	}
	// If node has no outgoing links, use 1 as a safe default.
	if minLA == math.MaxInt64 {
		minLA = 1
	}
	return minLA
}

// processEvent dispatches a single event to the appropriate algorithm
// handler and applies the resulting actions locally.
func (lp *LogicalProcess) processEvent(ev *Event) {
	switch ev.Type {

	case EventCrash:
		lp.node.SetStatus(StatusCrashed)
		lp.log.nodeCrash(lp.clock, lp.id)

	case EventNodeRecover:
		lp.node.SetStatus(StatusAlive)
		lp.node.ResetState()
		lp.log.nodeRecover(lp.clock, lp.id)
		buf := &runtimeBuffer{}
		lp.node.runtime = buf
		lp.algo.OnStart(lp.node)
		lp.log.nodeStart(lp.clock, lp.id)
		lp.applyActions(buf.actions)

	case EventLinkFail:
		lp.failed[[2]NodeID{ev.LinkFrom, ev.LinkTo}] = true
		lp.log.linkFail(lp.clock, ev.LinkFrom, ev.LinkTo)

	case EventLinkRecover:
		delete(lp.failed, [2]NodeID{ev.LinkFrom, ev.LinkTo})
		lp.log.linkRecover(lp.clock, ev.LinkFrom, ev.LinkTo)

	case EventStart:
		if lp.node.Status() == StatusCrashed {
			return
		}
		buf := &runtimeBuffer{}
		lp.node.runtime = buf
		lp.algo.OnStart(lp.node)
		lp.log.nodeStart(lp.clock, lp.id)
		lp.applyActions(buf.actions)

	case EventMessage:
		if lp.node.Status() == StatusCrashed {
			if ev.Msg != nil {
				lp.log.msgDropped(lp.clock, ev.Msg.From, ev.Msg.To, "node crashed")
			}
			return
		}
		buf := &runtimeBuffer{}
		lp.node.runtime = buf
		lp.algo.OnMessage(lp.node, *ev.Msg)
		lp.log.msgDeliver(lp.clock, ev.Msg.From, ev.Msg.To, ev.Msg.Payload)
		lp.applyActions(buf.actions)

	case EventTick:
		if lp.node.Status() == StatusCrashed {
			return
		}
		buf := &runtimeBuffer{}
		lp.node.runtime = buf
		lp.algo.OnTick(lp.node)
		lp.log.timerFire(lp.clock, lp.id)
		lp.applyActions(buf.actions)
	}
}

// applyActions is the local commit phase. It processes the actions produced
// by the algorithm handler:
//   - ActionSend: creates a message event and deposits it in the outbox
//     channel of the destination LP. The message will be picked up by the
//     destination LP at the start of the next barrier cycle (drainInbox).
//   - ActionSetTimer: inserts a tick event directly into this LP's local queue.
func (lp *LogicalProcess) applyActions(actions []Action) {
	for _, a := range actions {
		switch a.Type {

		case ActionSend:
			to := a.SendTo
			lp.log.msgSend(lp.clock, lp.id, to, a.Payload)

			// Check topology.
			if !lp.topo.HasEdge(lp.id, to) {
				lp.log.msgDropped(lp.clock, lp.id, to, "not a neighbor")
				continue
			}

			// Check link failure.
			if lp.failed[[2]NodeID{lp.id, to}] {
				lp.log.msgDropped(lp.clock, lp.id, to, "link failed")
				continue
			}

			delay := lp.delay.Delay(lp.id, to, a.Payload)
			if delay < 1 {
				delay = 1
			}
			deliverAt := lp.clock + delay

			msg := &Message{From: lp.id, To: to, Payload: a.Payload}
			ev := &Event{
				Time:   deliverAt,
				Type:   EventMessage,
				NodeID: to,
				Msg:    msg,
			}
			lp.log.msgScheduled(lp.clock, lp.id, to, deliverAt, a.Payload)

			// Deposit into destination LP's inbox queue (never blocks).
			if ub, ok := lp.outbox[to]; ok {
				ub.send(ev)
			}

		case ActionSetTimer:
			fireAt := lp.clock + a.Delay
			if a.Delay < 1 {
				fireAt = lp.clock + 1
			}
			lp.localQueue.Push(&Event{
				Time:   fireAt,
				Type:   EventTick,
				NodeID: lp.id,
			})
			lp.log.timerSet(lp.clock, lp.id, fireAt)
		}
	}
}

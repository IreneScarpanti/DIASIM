package core

import (
	"math"
	"sync"
)

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

	localQueue *EventQueue

	inbox map[NodeID]chan *Event

	outbox map[NodeID]chan *Event

	reportCh chan<- lpReport
	lbtsCh   <-chan int64
	doneCh   chan<- struct{}

	stepCount int
}

type lpReport struct {
	nodeID   NodeID
	value    int64
	finished bool
}

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
		inbox:      make(map[NodeID]chan *Event),
		outbox:     make(map[NodeID]chan *Event),
	}
}

func (lp *LogicalProcess) run(wg *sync.WaitGroup) {
	defer wg.Done()

	for {

		lp.drainInbox()

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

		lbts := <-lp.lbtsCh
		if lbts == math.MaxInt64 {
			return
		}

		for lp.localQueue.Len() > 0 && lp.localQueue.Peek().Time <= lbts {
			ev := lp.localQueue.Pop()
			lp.clock = ev.Time
			lp.stepCount++
			lp.processEvent(ev)
		}

		lp.doneCh <- struct{}{}
	}
}

func (lp *LogicalProcess) drainInbox() {
	for _, ch := range lp.inbox {
		for {
			select {
			case ev := <-ch:
				lp.localQueue.Push(ev)
			default:
				goto next
			}
		}
	next:
	}
}

func (lp *LogicalProcess) nextEventTime() int64 {
	if lp.localQueue.Len() == 0 {
		return math.MaxInt64
	}
	return lp.localQueue.Peek().Time
}

func (lp *LogicalProcess) minLookahead() int64 {
	minLA := int64(math.MaxInt64)
	for _, la := range lp.lookahead {
		if la < minLA {
			minLA = la
		}
	}
	if minLA == math.MaxInt64 {
		minLA = 1
	}
	return minLA
}

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

func (lp *LogicalProcess) applyActions(actions []Action) {
	for _, a := range actions {
		switch a.Type {

		case ActionSend:
			to := a.SendTo
			lp.log.msgSend(lp.clock, lp.id, to, a.Payload)

			if !lp.topo.HasEdge(lp.id, to) {
				lp.log.msgDropped(lp.clock, lp.id, to, "not a neighbor")
				continue
			}

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

			if ch, ok := lp.outbox[to]; ok {
				ch <- ev
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

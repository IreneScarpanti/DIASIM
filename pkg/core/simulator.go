package core

import (
	"io"
	"math/rand"
	"os"
	"sort"
)

type FailureConfig struct {
	NodeFailureRate    float64
	LinkFailureRate    float64
	FailureDurationMin int64
	FailureDurationMax int64
	FirstFailureAfter  int64
}

type SimConfig struct {
	Nodes     []NodeID
	Topology  TopologyReader
	Algorithm Algorithm
	Delay     DelayModel
	Seed      int64
	Failures  *FailureConfig
	LogLevel  Level
	LogOutput io.Writer
	BatchSize int
}

type runtimeBuffer struct{ actions []Action }

func (r *runtimeBuffer) AddAction(a Action) { r.actions = append(r.actions, a) }

type sideEffectKind string

const (
	sideEffectNodeCrash   sideEffectKind = "NODE_CRASH"
	sideEffectNodeRecover sideEffectKind = "NODE_RECOVER"
	sideEffectLinkFail    sideEffectKind = "LINK_FAIL"
	sideEffectLinkRecover sideEffectKind = "LINK_RECOVER"
)

type sideEffect struct {
	kind     sideEffectKind
	nodeID   NodeID
	linkFrom NodeID
	linkTo   NodeID
}

type eventKind string

const (
	eventKindStart   eventKind = "START"
	eventKindMessage eventKind = "MESSAGE"
	eventKindTick    eventKind = "TICK"
)

type computeResult struct {
	eventTime  int64
	kind       eventKind
	fromNode   NodeID
	actions    []Action
	sideEffect *sideEffect
	msg        *Message
}

type Simulator struct {
	cfg    SimConfig
	nodes  map[NodeID]*Node
	queue  *EventQueue
	seqGen int64
	time   int64
	steps  int
	rng    *rand.Rand
	failed map[[2]NodeID]bool
	log    *Logger
}

func New(cfg SimConfig) *Simulator {
	rng := rand.New(rand.NewSource(cfg.Seed))

	if cfg.Delay == nil {
		cfg.Delay = &FixedDelay{Min: 1, Max: 1}
	}
	switch d := cfg.Delay.(type) {
	case *FixedDelay:
		d.resolve(rng)
	case *PerLinkDelay:
		d.resolve(cfg.Topology, cfg.Nodes, rng)
	case *SeededDelay:
		d.init(rng)
	}

	out := cfg.LogOutput
	if out == nil {
		out = os.Stdout
	}

	s := &Simulator{
		cfg:    cfg,
		nodes:  make(map[NodeID]*Node),
		queue:  NewEventQueue(),
		rng:    rng,
		failed: make(map[[2]NodeID]bool),
		log:    newLogger(out, cfg.LogLevel),
	}

	for _, id := range cfg.Nodes {
		s.nodes[id] = NewNode(id, cfg.Topology, nil)
	}
	for _, id := range cfg.Nodes {
		s.pushEvent(&Event{Time: 0, Type: EventStart, NodeID: id})
	}

	if cfg.Failures != nil {
		s.scheduleProbabilisticFailures(cfg.Failures)
	}

	return s
}

func (s *Simulator) Logger() *Logger { return s.log }

func (s *Simulator) NodeState(id NodeID) *Node { return s.nodes[id] }

func (s *Simulator) scheduleProbabilisticFailures(fc *FailureConfig) {
	firstAt := fc.FirstFailureAfter
	if firstAt < 1 {
		firstAt = 1
	}
	durMin := fc.FailureDurationMin
	if durMin < 1 {
		durMin = 1
	}
	durMax := fc.FailureDurationMax
	if durMax < durMin {
		durMax = durMin
	}

	randDuration := func() int64 {
		if durMax == durMin {
			return durMin
		}
		return durMin + s.rng.Int63n(durMax-durMin+1)
	}

	if fc.NodeFailureRate > 0 {
		ids := make([]NodeID, len(s.cfg.Nodes))
		copy(ids, s.cfg.Nodes)
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		s.rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

		count := int(float64(len(ids)) * fc.NodeFailureRate)
		for i := 0; i < count; i++ {
			id := ids[i]
			crashAt := firstAt + s.rng.Int63n(10) + 1
			recoverAt := crashAt + randDuration()
			s.pushEvent(&Event{Time: crashAt, Type: EventCrash, NodeID: id})
			s.pushEvent(&Event{Time: recoverAt, Type: EventNodeRecover, NodeID: id})
		}
	}

	if fc.LinkFailureRate > 0 {
		type edge struct{ from, to NodeID }
		var edges []edge
		for _, from := range s.cfg.Nodes {
			for _, to := range s.cfg.Topology.Neighbors(from) {
				edges = append(edges, edge{from, to})
			}
		}
		sort.Slice(edges, func(i, j int) bool {
			if edges[i].from != edges[j].from {
				return edges[i].from < edges[j].from
			}
			return edges[i].to < edges[j].to
		})
		s.rng.Shuffle(len(edges), func(i, j int) { edges[i], edges[j] = edges[j], edges[i] })

		count := int(float64(len(edges)) * fc.LinkFailureRate)
		for i := 0; i < count; i++ {
			e := edges[i]
			failAt := firstAt + s.rng.Int63n(10) + 1
			recoverAt := failAt + randDuration()
			s.pushEvent(&Event{Time: failAt, Type: EventLinkFail, LinkFrom: e.from, LinkTo: e.to})
			s.pushEvent(&Event{Time: recoverAt, Type: EventLinkRecover, LinkFrom: e.from, LinkTo: e.to})
		}
	}
}

func (s *Simulator) pushEvent(e *Event) {
	s.seqGen++
	e.SeqNum = s.seqGen
	s.queue.Push(e)
}

func (s *Simulator) Run() int {
	s.log.simStart(s.time, len(s.cfg.Nodes))

	batchSize := s.cfg.BatchSize
	if batchSize < 1 {
		batchSize = 1
	}

	for s.queue.Len() > 0 {
		// All events in the batch must share the same logical timestamp.
		results := make([]computeResult, 0, batchSize)
		currentTime := s.queue.Peek().Time

		for i := 0; i < batchSize && s.queue.Len() > 0; i++ {
			if s.queue.Peek().Time != currentTime {
				break
			}
			ev := s.queue.Pop()
			s.steps++
			result := s.compute(ev)
			result.eventTime = ev.Time
			results = append(results, result)
		}

		for _, result := range results {
			s.commit(result)
		}
	}

	s.log.simEnd(s.time, s.steps)
	return s.steps
}

func (s *Simulator) compute(ev *Event) computeResult {
	switch ev.Type {

	case EventCrash:
		return computeResult{
			sideEffect: &sideEffect{kind: sideEffectNodeCrash, nodeID: ev.NodeID},
		}

	case EventNodeRecover:
		n := s.nodes[ev.NodeID]
		if n == nil {
			return computeResult{}
		}
		buf := &runtimeBuffer{}
		n.runtime = buf
		n.ResetState()
		s.cfg.Algorithm.OnStart(n)
		return computeResult{
			kind:     eventKindStart,
			fromNode: ev.NodeID,
			actions:  buf.actions,
			sideEffect: &sideEffect{
				kind:   sideEffectNodeRecover,
				nodeID: ev.NodeID,
			},
		}

	case EventLinkFail:
		return computeResult{
			sideEffect: &sideEffect{
				kind:     sideEffectLinkFail,
				linkFrom: ev.LinkFrom,
				linkTo:   ev.LinkTo,
			},
		}

	case EventLinkRecover:
		return computeResult{
			sideEffect: &sideEffect{
				kind:     sideEffectLinkRecover,
				linkFrom: ev.LinkFrom,
				linkTo:   ev.LinkTo,
			},
		}

	case EventStart:
		n := s.nodes[ev.NodeID]
		if n == nil {
			return computeResult{}
		}
		buf := &runtimeBuffer{}
		n.runtime = buf
		s.cfg.Algorithm.OnStart(n)
		return computeResult{
			kind:     eventKindStart,
			fromNode: ev.NodeID,
			actions:  buf.actions,
		}

	case EventMessage:
		n := s.nodes[ev.NodeID]
		if n == nil {
			return computeResult{}
		}
		buf := &runtimeBuffer{}
		n.runtime = buf
		s.cfg.Algorithm.OnMessage(n, *ev.Msg)
		return computeResult{
			kind:     eventKindMessage,
			fromNode: ev.NodeID,
			actions:  buf.actions,
			msg:      ev.Msg,
		}

	case EventTick:
		n := s.nodes[ev.NodeID]
		if n == nil {
			return computeResult{}
		}
		buf := &runtimeBuffer{}
		n.runtime = buf
		s.cfg.Algorithm.OnTick(n)
		return computeResult{
			kind:     eventKindTick,
			fromNode: ev.NodeID,
			actions:  buf.actions,
		}
	}

	return computeResult{}
}

func (s *Simulator) commit(result computeResult) {
	t := result.eventTime

	if se := result.sideEffect; se != nil {
		switch se.kind {
		case sideEffectNodeCrash:
			if n := s.nodes[se.nodeID]; n != nil {
				n.SetStatus(StatusCrashed)
				s.log.nodeCrash(t, se.nodeID)
			}
		case sideEffectNodeRecover:
			if n := s.nodes[se.nodeID]; n != nil {
				n.SetStatus(StatusAlive)
				s.log.nodeRecover(t, se.nodeID)
			}
		case sideEffectLinkFail:
			s.failed[[2]NodeID{se.linkFrom, se.linkTo}] = true
			s.log.linkFail(t, se.linkFrom, se.linkTo)
		case sideEffectLinkRecover:
			delete(s.failed, [2]NodeID{se.linkFrom, se.linkTo})
			s.log.linkRecover(t, se.linkFrom, se.linkTo)
		}
	}

	from := result.fromNode
	if from != "" {
		n := s.nodes[from]
		if n == nil || n.Status() == StatusCrashed {
			if result.kind == eventKindMessage && result.msg != nil {
				s.log.msgDropped(t, result.msg.From, result.msg.To, "node crashed")
			}
			s.time = t
			return
		}
	}

	switch result.kind {
	case eventKindStart:
		s.log.nodeStart(t, from)
	case eventKindMessage:
		if result.msg != nil {
			s.log.msgDeliver(t, result.msg.From, result.msg.To, result.msg.Payload)
		}
	case eventKindTick:
		s.log.timerFire(t, from)
	}

	for _, a := range result.actions {
		switch a.Type {

		case ActionSend:
			to := a.SendTo
			s.log.msgSend(t, from, to, a.Payload)
			if !s.cfg.Topology.HasEdge(from, to) {
				s.log.msgDropped(t, from, to, "not a neighbor")
				continue
			}
			if s.failed[[2]NodeID{from, to}] {
				s.log.msgDropped(t, from, to, "link failed")
				continue
			}
			delay := s.cfg.Delay.Delay(from, to, a.Payload)
			if delay < 1 {
				delay = 1
			}
			deliverAt := t + delay
			msg := &Message{From: from, To: to, Payload: a.Payload}
			s.pushEvent(&Event{Time: deliverAt, Type: EventMessage, NodeID: to, Msg: msg})
			s.log.msgScheduled(t, from, to, deliverAt, a.Payload)

		case ActionSetTimer:
			fireAt := t + a.Delay
			s.pushEvent(&Event{Time: fireAt, Type: EventTick, NodeID: from})
			s.log.timerSet(t, from, fireAt)
		}
	}

	s.time = t
}

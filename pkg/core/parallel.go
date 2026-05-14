package core

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

// runParallel implements the CMB (Chandy-Misra-Bryant) parallel simulation
// with barrier synchronization. The Simulator acts as the coordinator:
//
//  1. It creates one LogicalProcess (LP) per node, each with its own local
//     event queue and goroutine.
//  2. LPs communicate via buffered Go channels (one per directed edge in the
//     topology).
//  3. At each barrier cycle the coordinator collects LBTS contributions from
//     all LPs, computes the global minimum, broadcasts it back, and waits
//     for all LPs to finish processing their safe events.
//  4. When all LPs report they are idle (no local events, empty inboxes),
//     the coordinator sends a termination signal and the simulation ends.
//
// The coordinator does NOT process events itself — that is entirely delegated
// to the LP goroutines.
func (s *Simulator) runParallel() int {
	numNodes := len(s.cfg.Nodes)
	s.log.simStart(0, numNodes)

	// ── Build lookahead table ─────────────────────────────────────────────
	lookaheadTable := computeLookahead(s.cfg.Delay, s.cfg.Topology, s.cfg.Nodes)

	// ── Create LPs ────────────────────────────────────────────────────────
	lps := make(map[NodeID]*LogicalProcess, numNodes)
	for i, id := range s.cfg.Nodes {
		// Per-LP lookahead: only the outgoing links of this node.
		lpLA := make(map[NodeID]int64)
		for _, nb := range s.cfg.Topology.Neighbors(id) {
			lpLA[nb] = lookaheadTable[[2]NodeID{id, nb}]
		}

		// Per-LP delay model: SeededDelay contains a shared *rand.Rand that
		// is not goroutine-safe. Each LP gets its own copy with an independent
		// RNG derived from the simulation seed + LP index, ensuring no data
		// race while preserving reproducibility per-LP.
		// FixedDelay and PerLinkDelay are read-only after resolution, so they
		// can be shared safely.
		lpDelay := s.cfg.Delay
		if sd, ok := s.cfg.Delay.(*SeededDelay); ok {
			lpDelay = &SeededDelay{
				Min: sd.Min,
				Max: sd.Max,
				rng: rand.New(rand.NewSource(s.cfg.Seed + int64(i) + 1)),
			}
		}

		lps[id] = newLogicalProcess(
			id,
			s.nodes[id],
			s.cfg.Algorithm,
			lpDelay,
			s.cfg.Topology,
			s.log,
			lpLA,
		)
	}

	// ── Create inter-LP inboxes (one per directed edge) ──────────────────
	// Each inbox is an unbounded thread-safe queue. Senders never block,
	// which prevents deadlocks when an LP generates many messages in a
	// single barrier cycle.
	for _, from := range s.cfg.Nodes {
		for _, to := range s.cfg.Topology.Neighbors(from) {
			// Create an inbox for the directed edge from → to.
			if _, exists := lps[to].inbox[from]; !exists {
				ub := newUnboundedInbox()
				lps[to].inbox[from] = ub  // to reads from this inbox
				lps[from].outbox[to] = ub // from writes to this inbox
			}
		}
	}

	// ── Distribute initial events from the global queue to LP local queues ─
	// The global queue was populated by New() with EventStart and failure
	// events. We drain it and route each event to the appropriate LP.
	for s.queue.Len() > 0 {
		ev := s.queue.Pop()
		targetID := ev.NodeID
		// Link failure events use LinkFrom as the owning LP (the LP whose
		// outgoing link is affected).
		if ev.Type == EventLinkFail || ev.Type == EventLinkRecover {
			targetID = ev.LinkFrom
		}
		if lp, ok := lps[targetID]; ok {
			lp.localQueue.Push(ev)
		}
	}

	// ── Barrier channels ──────────────────────────────────────────────────
	reportCh := make(chan lpReport, numNodes)
	doneCh := make(chan struct{}, numNodes)
	lbtsChs := make(map[NodeID]chan int64, numNodes)
	for _, id := range s.cfg.Nodes {
		lbtsChs[id] = make(chan int64, 1)
	}

	// Wire barrier channels into each LP.
	for id, lp := range lps {
		lp.reportCh = reportCh
		lp.lbtsCh = lbtsChs[id]
		lp.doneCh = doneCh
	}

	// ── Launch LP goroutines ──────────────────────────────────────────────
	var wg sync.WaitGroup
	for _, lp := range lps {
		wg.Add(1)
		go lp.run(&wg)
	}

	// ── Coordinator barrier loop ──────────────────────────────────────────
	var totalSteps atomic.Int64
	for {
		// Collect LBTS reports from all LPs.
		minLBTS := int64(math.MaxInt64)
		allFinished := true
		for i := 0; i < numNodes; i++ {
			r := <-reportCh
			if !r.finished {
				allFinished = false
				if r.value < minLBTS {
					minLBTS = r.value
				}
			}
		}

		if allFinished {
			// All LPs are idle: broadcast termination.
			for _, ch := range lbtsChs {
				ch <- math.MaxInt64
			}
			break
		}

		// Broadcast LBTS to all LPs.
		for _, ch := range lbtsChs {
			ch <- minLBTS
		}

		// Wait for all LPs to finish processing their safe events.
		for i := 0; i < numNodes; i++ {
			<-doneCh
		}
	}

	// ── Wait for all goroutines to exit ───────────────────────────────────
	wg.Wait()

	// ── Collect step counts ───────────────────────────────────────────────
	for _, lp := range lps {
		totalSteps.Add(int64(lp.stepCount))
	}
	s.steps = int(totalSteps.Load())

	s.log.simEnd(0, s.steps)
	return s.steps
}

// computeLookahead derives the lookahead value for each directed edge in the
// topology from the configured DelayModel.
//
// The lookahead is the minimum delay guaranteed on a link — it is the lower
// bound on the timestamp of any message that will be sent on that link.
//
//   - FixedDelay: the resolved fixed value applies to every link.
//   - PerLinkDelay: each link has its own pre-resolved delay (which is also
//     the minimum, since it's constant per link).
//   - SeededDelay: the minimum possible delay (Min field) is used because
//     each individual message may get any delay in [Min, Max].
//   - Fallback: 1 (the minimum enforced by the simulator).
func computeLookahead(dm DelayModel, topo TopologyReader, nodes []NodeID) map[[2]NodeID]int64 {
	la := make(map[[2]NodeID]int64)

	for _, from := range nodes {
		for _, to := range topo.Neighbors(from) {
			key := [2]NodeID{from, to}
			switch d := dm.(type) {
			case *FixedDelay:
				// FixedDelay resolves to a single value for all links.
				la[key] = d.Delay(from, to, nil)
			case *PerLinkDelay:
				// PerLinkDelay has a constant delay per link.
				la[key] = d.Delay(from, to, nil)
			case *SeededDelay:
				// SeededDelay picks a random value in [Min, Max] per message.
				// The lookahead is the worst-case minimum: Min.
				v := d.Min
				if v < 1 {
					v = 1
				}
				la[key] = v
			default:
				la[key] = 1
			}
		}
	}
	return la
}

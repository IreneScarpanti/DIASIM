package core

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

func (s *Simulator) runParallel() int {
	numNodes := len(s.cfg.Nodes)
	s.log.simStart(0, numNodes)

	lookaheadTable := computeLookahead(s.cfg.Delay, s.cfg.Topology, s.cfg.Nodes)

	lps := make(map[NodeID]*LogicalProcess, numNodes)
	for i, id := range s.cfg.Nodes {
		lpLA := make(map[NodeID]int64)
		for _, nb := range s.cfg.Topology.Neighbors(id) {
			lpLA[nb] = lookaheadTable[[2]NodeID{id, nb}]
		}

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

	chanBuf := numNodes * 4
	if chanBuf < 16 {
		chanBuf = 16
	}
	for _, from := range s.cfg.Nodes {
		for _, to := range s.cfg.Topology.Neighbors(from) {

			if _, exists := lps[to].inbox[from]; !exists {
				ch := make(chan *Event, chanBuf)
				lps[to].inbox[from] = ch
				lps[from].outbox[to] = ch
			}
		}
	}

	for s.queue.Len() > 0 {
		ev := s.queue.Pop()
		targetID := ev.NodeID

		if ev.Type == EventLinkFail || ev.Type == EventLinkRecover {
			targetID = ev.LinkFrom
		}
		if lp, ok := lps[targetID]; ok {
			lp.localQueue.Push(ev)
		}
	}

	reportCh := make(chan lpReport, numNodes)
	doneCh := make(chan struct{}, numNodes)
	lbtsChs := make(map[NodeID]chan int64, numNodes)
	for _, id := range s.cfg.Nodes {
		lbtsChs[id] = make(chan int64, 1)
	}

	for id, lp := range lps {
		lp.reportCh = reportCh
		lp.lbtsCh = lbtsChs[id]
		lp.doneCh = doneCh
	}

	var wg sync.WaitGroup
	for _, lp := range lps {
		wg.Add(1)
		go lp.run(&wg)
	}

	var totalSteps atomic.Int64
	for {
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
			for _, ch := range lbtsChs {
				ch <- math.MaxInt64
			}
			break
		}

		for _, ch := range lbtsChs {
			ch <- minLBTS
		}

		for i := 0; i < numNodes; i++ {
			<-doneCh
		}
	}

	wg.Wait()

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
// The lookahead is the minimum delay guaranteed on a link, it is the lower
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
				la[key] = d.Delay(from, to, nil)
			case *PerLinkDelay:
				la[key] = d.Delay(from, to, nil)
			case *SeededDelay:
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

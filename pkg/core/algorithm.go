package core

import "math/rand"

// OnStart  – called once when a node is initialised.
// OnMessage – called when a message is delivered to a node.
// OnTick   – called when a timer fires on a node.
type Algorithm interface {
	OnStart(n *Node)
	OnMessage(n *Node, msg Message)
	OnTick(n *Node)
}

type DelayModel interface {
	Delay(from, to NodeID, payload any) int64
}

// FixedDelay picks a single value uniformly at random from [Min, Max] using
// the simulator seed, and applies that same value to every message for the
// entire simulation.
type FixedDelay struct {
	Min   int64
	Max   int64
	value int64 // resolved once by the simulator
	ready bool
}

// resolve picks the fixed value using rng. Called once by the simulator.
func (f *FixedDelay) resolve(rng *rand.Rand) {
	if f.ready {
		return
	}
	if f.Max <= f.Min {
		f.value = f.Min
	} else {
		f.value = f.Min + rng.Int63n(f.Max-f.Min+1)
	}
	f.ready = true
}

func (f *FixedDelay) Delay(_, _ NodeID, _ any) int64 { return f.value }

// PerLinkDelay assigns an independently chosen pseudo-random delay to each
// directed link (from, to), drawn uniformly from [Min, Max] using the
// simulator seed. The same delay is used for all messages on that link
// throughout the simulation.
type PerLinkDelay struct {
	Min    int64
	Max    int64
	delays map[[2]NodeID]int64 // assigned once by the simulator via resolve()
}

func (p *PerLinkDelay) resolve(topo TopologyReader, nodes []NodeID, rng *rand.Rand) {
	p.delays = make(map[[2]NodeID]int64)
	for _, from := range nodes {
		for _, to := range topo.Neighbors(from) {
			var d int64
			if p.Max <= p.Min {
				d = p.Min
			} else {
				d = p.Min + rng.Int63n(p.Max-p.Min+1)
			}
			p.delays[[2]NodeID{from, to}] = d
		}
	}
}

func (p *PerLinkDelay) Delay(from, to NodeID, _ any) int64 {
	return p.delays[[2]NodeID{from, to}]
}

// SeededDelay produces a fresh pseudo-random delay in [Min, Max] for every
// message, using the simulator's seed for full reproducibility.
//
// The simulator initialises the internal RNG automatically — the user only
// specifies the range.
type SeededDelay struct {
	Min int64
	Max int64
	rng *rand.Rand // set by the simulator via init()
}

func (sd *SeededDelay) init(rng *rand.Rand) { sd.rng = rng }

func (sd *SeededDelay) Delay(_, _ NodeID, _ any) int64 {
	if sd.rng == nil || sd.Max <= sd.Min {
		return sd.Min
	}
	return sd.Min + sd.rng.Int63n(sd.Max-sd.Min+1)
}

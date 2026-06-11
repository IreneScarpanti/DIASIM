// Package flooding_naive implements a simple flooding algorithm with no
// fault tolerance. A node receives a value and forwards it to all neighbors
// exactly once. There are no ACKs, no retries, and no mechanism to detect
// or recover from any type of failure.
//
// This is the simplest possible broadcast algorithm and serves as a baseline
// for comparison with fault-tolerant variants.
package flooding_naive

import (
	"fmt"

	"diasim/pkg/core"
)

const (
	stateReceived = "naive_received"
	stateValue    = "naive_value"
)

type Msg struct {
	Value any
}

func (m Msg) String() string {
	return fmt.Sprintf("NAIVE_FLOOD(%v)", m.Value)
}

type Algorithm struct {
	Initiator core.NodeID
	Value     any
}

func (a *Algorithm) OnStart(n *core.Node) {
	if n.ID() == a.Initiator {
		n.Set(stateReceived, true)
		n.Set(stateValue, a.Value)
		for _, nb := range n.Neighbors() {
			n.Send(nb, Msg{Value: a.Value})
		}
	}
}

func (a *Algorithm) OnMessage(n *core.Node, msg core.Message) {
	payload, ok := msg.Payload.(Msg)
	if !ok {
		return
	}

	if rcv, _ := n.Get(stateReceived); rcv == true {
		return // already received, ignore
	}

	n.Set(stateReceived, true)
	n.Set(stateValue, payload.Value)

	// Forward to all neighbors except sender.
	for _, nb := range n.Neighbors() {
		if nb == msg.From {
			continue
		}
		n.Send(nb, Msg{Value: payload.Value})
	}
}

func (a *Algorithm) OnTick(n *core.Node) {
	// No timers used.
}

// ── Verification helpers ─────────────────────────────────────────────────────

func Received(sim *core.Simulator, id core.NodeID) bool {
	n := sim.NodeState(id)
	if n == nil {
		return false
	}
	v, _ := n.Get(stateReceived)
	return v == true
}

func ReceivedValue(sim *core.Simulator, id core.NodeID) (any, bool) {
	n := sim.NodeState(id)
	if n == nil {
		return nil, false
	}
	v, ok := n.Get(stateValue)
	return v, ok
}

// CountCorrectDeliveries returns how many non-crashed, non-Byzantine nodes
// received the correct value.
func CountCorrectDeliveries(sim *core.Simulator, ids []core.NodeID, expectedValue any) (delivered, total int) {
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		total++
		v, ok := n.Get(stateValue)
		if ok && v == expectedValue {
			delivered++
		}
	}
	return
}

package flooding

import (
	"fmt"

	"diasim/pkg/core"
)

const (
	stateReceived = "flood_received"

	stateValue = "flood_value"

	stateForwarded = "flood_forwarded"

	statePending = "flood_pending"
)

const retryInterval = 5

type msgKind string

const (
	kindFlood msgKind = "FLOOD"
	kindAck   msgKind = "ACK"
	kindHello msgKind = "HELLO"
)

type Msg struct {
	Kind  msgKind
	Value any
}

func (m Msg) String() string {
	switch m.Kind {
	case kindAck:
		return "FLOOD_ACK"
	case kindHello:
		return "FLOOD_HELLO"
	default:
		return fmt.Sprintf("FLOOD(%v)", m.Value)
	}
}

type Algorithm struct {
	Initiator core.NodeID

	Value any
}

func (a *Algorithm) OnStart(n *core.Node) {
	n.Set(stateForwarded, map[core.NodeID]bool{})
	n.Set(statePending, map[core.NodeID]bool{})

	if n.ID() == a.Initiator {
		n.Set(stateReceived, true)
		n.Set(stateValue, a.Value)
		a.floodToNeighbors(n, "", a.Value)
		return
	}

	for _, nb := range n.Neighbors() {
		n.Send(nb, Msg{Kind: kindHello})
	}
}

func (a *Algorithm) OnMessage(n *core.Node, msg core.Message) {
	payload, ok := msg.Payload.(Msg)
	if !ok {
		return
	}

	switch payload.Kind {

	case kindFlood:
		a.handleFlood(n, msg.From, payload.Value)

	case kindAck:
		a.handleAck(n, msg.From)

	case kindHello:
		a.handleHello(n, msg.From)
	}
}

func (a *Algorithm) OnTick(n *core.Node) {
	pending := getPending(n)
	if len(pending) == 0 {
		return
	}

	v, _ := n.Get(stateValue)
	for nb := range pending {
		n.Send(nb, Msg{Kind: kindFlood, Value: v})
	}
	n.SetTimer(retryInterval)
}

func (a *Algorithm) handleFlood(n *core.Node, from core.NodeID, value any) {
	n.Send(from, Msg{Kind: kindAck})

	if rcv, _ := n.Get(stateReceived); rcv == true {
		return
	}

	n.Set(stateReceived, true)
	n.Set(stateValue, value)
	a.floodToNeighbors(n, from /* exclude sender */, value)
}

func (a *Algorithm) handleAck(n *core.Node, from core.NodeID) {
	removePending(n, from)
}

func (a *Algorithm) handleHello(n *core.Node, from core.NodeID) {
	rcv, _ := n.Get(stateReceived)
	if rcv != true {
		return
	}

	pending := getPending(n)
	if pending[from] {
		return
	}

	v, _ := n.Get(stateValue)
	n.Send(from, Msg{Kind: kindFlood, Value: v})

	forwarded := getForwarded(n)
	forwarded[from] = true
	pending[from] = true
	n.Set(stateForwarded, forwarded)
	n.Set(statePending, pending)
	n.SetTimer(retryInterval)
}

func (a *Algorithm) floodToNeighbors(n *core.Node, exclude core.NodeID, value any) {
	forwarded := getForwarded(n)
	pending := getPending(n)

	sent := false
	for _, nb := range n.Neighbors() {
		if nb == exclude || forwarded[nb] {
			continue
		}
		n.Send(nb, Msg{Kind: kindFlood, Value: value})
		forwarded[nb] = true
		pending[nb] = true
		sent = true
	}

	n.Set(stateForwarded, forwarded)
	n.Set(statePending, pending)

	if sent {
		n.SetTimer(retryInterval)
	}
}

func getPending(n *core.Node) map[core.NodeID]bool {
	v, _ := n.Get(statePending)
	if m, ok := v.(map[core.NodeID]bool); ok {
		return m
	}
	return map[core.NodeID]bool{}
}

func getForwarded(n *core.Node) map[core.NodeID]bool {
	v, _ := n.Get(stateForwarded)
	if m, ok := v.(map[core.NodeID]bool); ok {
		return m
	}
	return map[core.NodeID]bool{}
}

func removePending(n *core.Node, id core.NodeID) {
	pending := getPending(n)
	delete(pending, id)
	n.Set(statePending, pending)
}

// ── verification helpers (used in tests) ─────────────────────────────────────

// AllReceived returns true if every non-crashed node in ids has received the
// flooded value.  Permanently crashed nodes are excluded from the check.
func AllReceived(sim *core.Simulator, ids []core.NodeID) bool {
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed {
			continue
		}
		v, ok := n.Get(stateReceived)
		if !ok || v != true {
			return false
		}
	}
	return true
}

// Received returns whether node id has received the flooded value.
func Received(sim *core.Simulator, id core.NodeID) bool {
	n := sim.NodeState(id)
	if n == nil {
		return false
	}
	v, _ := n.Get(stateReceived)
	return v == true
}

// Package bracha implements Bracha's Reliable Broadcast algorithm for the
// DIASIM simulator.
//
// Bracha's algorithm guarantees reliable broadcast in the presence of up to
// f < n/3 Byzantine nodes. It operates in three phases:
//
//  1. SEND: The broadcaster sends the value to all nodes.
//  2. ECHO: Upon receiving SEND from the broadcaster, each node echoes
//     the value to all nodes.
//  3. READY: When a node receives enough ECHOs (quorum threshold), it sends
//     READY to all nodes. Also triggered by receiving f+1 READY messages.
//  4. DELIVER: When a node receives 2f+1 READY messages, it delivers the value.
//
// The algorithm ensures:
//   - Validity: if the broadcaster is correct, all correct nodes deliver the value.
//   - Agreement: if any correct node delivers v, all correct nodes deliver v.
//   - Integrity: each correct node delivers at most one value.
//
// The parameter f (max Byzantine faults) is provided in the configuration.
// The algorithm requires n > 3f to guarantee correctness.
package bracha

import (
	"fmt"

	"diasim/pkg/core"
)

// ── Message types ────────────────────────────────────────────────────────────

type msgKind string

const (
	kindSend  msgKind = "SEND"
	kindEcho  msgKind = "ECHO"
	kindReady msgKind = "READY"
)

type Msg struct {
	Kind  msgKind
	Value any
}

func (m Msg) String() string {
	return fmt.Sprintf("BRACHA_%s(%v)", m.Kind, m.Value)
}

// ── State keys ───────────────────────────────────────────────────────────────

const (
	stateSendReceived = "bracha_sendReceived" // bool: received SEND from broadcaster
	stateEchoSent     = "bracha_echoSent"     // bool: sent ECHO
	stateReadySent    = "bracha_readySent"    // bool: sent READY
	stateDelivered    = "bracha_delivered"    // bool: delivered the value
	stateValue        = "bracha_value"        // the delivered value
	stateEchoCount    = "bracha_echoCount"    // map[any]map[NodeID]bool: echo counts per value
	stateReadyCount   = "bracha_readyCount"   // map[any]map[NodeID]bool: ready counts per value
	stateF            = "bracha_f"            // int: max Byzantine faults
	stateN            = "bracha_n"            // int: total number of nodes
)

// ── Algorithm ────────────────────────────────────────────────────────────────

// Algorithm implements Bracha's Reliable Broadcast.
type Algorithm struct {
	// Broadcaster is the node that initiates the broadcast.
	Broadcaster core.NodeID

	// Value is the value to broadcast.
	Value any

	// F is the maximum number of Byzantine faults tolerated.
	// The algorithm requires n > 3*F to guarantee correctness.
	F int
}

func (a *Algorithm) OnStart(n *core.Node) {
	allNodes := append([]core.NodeID{n.ID()}, n.Neighbors()...)
	nTotal := len(allNodes)

	n.Set(stateSendReceived, false)
	n.Set(stateEchoSent, false)
	n.Set(stateReadySent, false)
	n.Set(stateDelivered, false)
	n.Set(stateF, a.F)
	n.Set(stateN, nTotal)
	// Use string keys for map (any is not comparable for map keys in all cases)
	n.Set(stateEchoCount, make(map[string]map[core.NodeID]bool))
	n.Set(stateReadyCount, make(map[string]map[core.NodeID]bool))

	// Broadcaster sends SEND to all neighbors and immediately echoes
	// (as if it received its own SEND).
	if n.ID() == a.Broadcaster {
		n.Set(stateSendReceived, true)
		n.Set(stateEchoSent, true)
		for _, nb := range n.Neighbors() {
			n.Send(nb, Msg{Kind: kindSend, Value: a.Value})
			n.Send(nb, Msg{Kind: kindEcho, Value: a.Value})
		}
		// Count own echo.
		vKey := valueKey(a.Value)
		echoCount := getEchoCount(n)
		if echoCount[vKey] == nil {
			echoCount[vKey] = make(map[core.NodeID]bool)
		}
		echoCount[vKey][n.ID()] = true
		n.Set(stateEchoCount, echoCount)
	}
}

func (a *Algorithm) OnMessage(n *core.Node, msg core.Message) {
	payload, ok := msg.Payload.(Msg)
	if !ok {
		return
	}

	switch payload.Kind {
	case kindSend:
		a.handleSend(n, msg.From, payload.Value)
	case kindEcho:
		a.handleEcho(n, msg.From, payload.Value)
	case kindReady:
		a.handleReady(n, msg.From, payload.Value)
	}
}

func (a *Algorithm) OnTick(n *core.Node) {
	// No timers used.
}

// ── Phase handlers ───────────────────────────────────────────────────────────

func (a *Algorithm) handleSend(n *core.Node, from core.NodeID, value any) {
	// Only accept SEND from the designated broadcaster.
	if from != a.Broadcaster {
		return
	}

	sendReceived, _ := n.Get(stateSendReceived)
	if sendReceived == true {
		return // already processed SEND
	}

	n.Set(stateSendReceived, true)

	// Send ECHO to all neighbors and count own echo.
	echoSent, _ := n.Get(stateEchoSent)
	if echoSent != true {
		n.Set(stateEchoSent, true)
		for _, nb := range n.Neighbors() {
			n.Send(nb, Msg{Kind: kindEcho, Value: value})
		}
		// Count own echo.
		vKey := valueKey(value)
		echoCount := getEchoCount(n)
		if echoCount[vKey] == nil {
			echoCount[vKey] = make(map[core.NodeID]bool)
		}
		echoCount[vKey][n.ID()] = true
		n.Set(stateEchoCount, echoCount)
		a.checkThresholds(n, value)
	}
}

func (a *Algorithm) handleEcho(n *core.Node, from core.NodeID, value any) {
	vKey := valueKey(value)
	echoCount := getEchoCount(n)

	if echoCount[vKey] == nil {
		echoCount[vKey] = make(map[core.NodeID]bool)
	}
	echoCount[vKey][from] = true
	n.Set(stateEchoCount, echoCount)

	a.checkThresholds(n, value)
}

func (a *Algorithm) handleReady(n *core.Node, from core.NodeID, value any) {
	vKey := valueKey(value)
	readyCount := getReadyCount(n)

	if readyCount[vKey] == nil {
		readyCount[vKey] = make(map[core.NodeID]bool)
	}
	readyCount[vKey][from] = true
	n.Set(stateReadyCount, readyCount)

	a.checkThresholds(n, value)
}

// checkThresholds checks if ECHO or READY quorum thresholds are met
// and triggers the appropriate transitions.
func (a *Algorithm) checkThresholds(n *core.Node, value any) {
	vKey := valueKey(value)
	f := getF(n)
	nTotal := getN(n)

	echoCount := getEchoCount(n)
	readyCount := getReadyCount(n)

	echos := len(echoCount[vKey])
	readies := len(readyCount[vKey])

	// Threshold for ECHO → READY: more than (n + f) / 2
	echoThreshold := (nTotal + f) / 2

	readySent, _ := n.Get(stateReadySent)

	// If we have enough ECHOs and haven't sent READY yet, send READY.
	if readySent != true && echos > echoThreshold {
		n.Set(stateReadySent, true)
		for _, nb := range n.Neighbors() {
			n.Send(nb, Msg{Kind: kindReady, Value: value})
		}
		// Count own READY.
		if readyCount[vKey] == nil {
			readyCount[vKey] = make(map[core.NodeID]bool)
		}
		readyCount[vKey][n.ID()] = true
		n.Set(stateReadyCount, readyCount)
		readies = len(readyCount[vKey])
	}

	// If we received f+1 READYs and haven't sent READY yet, amplify.
	readySent, _ = n.Get(stateReadySent)
	if readySent != true && readies >= f+1 {
		n.Set(stateReadySent, true)
		for _, nb := range n.Neighbors() {
			n.Send(nb, Msg{Kind: kindReady, Value: value})
		}
		// Count own READY.
		if readyCount[vKey] == nil {
			readyCount[vKey] = make(map[core.NodeID]bool)
		}
		readyCount[vKey][n.ID()] = true
		n.Set(stateReadyCount, readyCount)
		readies = len(readyCount[vKey])
	}

	// If we have 2f+1 READYs, deliver.
	delivered, _ := n.Get(stateDelivered)
	if delivered != true && readies >= 2*f+1 {
		n.Set(stateDelivered, true)
		n.Set(stateValue, value)
	}
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// valueKey converts a value to a string key for map indexing.
func valueKey(v any) string {
	return fmt.Sprintf("%v", v)
}

func getEchoCount(n *core.Node) map[string]map[core.NodeID]bool {
	v, _ := n.Get(stateEchoCount)
	if m, ok := v.(map[string]map[core.NodeID]bool); ok {
		return m
	}
	return make(map[string]map[core.NodeID]bool)
}

func getReadyCount(n *core.Node) map[string]map[core.NodeID]bool {
	v, _ := n.Get(stateReadyCount)
	if m, ok := v.(map[string]map[core.NodeID]bool); ok {
		return m
	}
	return make(map[string]map[core.NodeID]bool)
}

func getF(n *core.Node) int {
	v, _ := n.Get(stateF)
	if i, ok := v.(int); ok {
		return i
	}
	return 0
}

func getN(n *core.Node) int {
	v, _ := n.Get(stateN)
	if i, ok := v.(int); ok {
		return i
	}
	return 1
}

// ── Verification helpers ─────────────────────────────────────────────────────

// Delivered returns true if the node delivered a value.
func Delivered(sim *core.Simulator, id core.NodeID) bool {
	n := sim.NodeState(id)
	if n == nil {
		return false
	}
	v, _ := n.Get(stateDelivered)
	return v == true
}

// DeliveredValue returns the value delivered by the node, if any.
func DeliveredValue(sim *core.Simulator, id core.NodeID) (any, bool) {
	n := sim.NodeState(id)
	if n == nil {
		return nil, false
	}
	v, ok := n.Get(stateValue)
	return v, ok
}

// CountCorrectDeliveries returns how many non-crashed, non-Byzantine nodes
// delivered the correct value.
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

// AllCorrectDelivered returns true if all non-crashed, non-Byzantine nodes
// delivered the correct value.
func AllCorrectDelivered(sim *core.Simulator, ids []core.NodeID, expectedValue any) bool {
	d, t := CountCorrectDeliveries(sim, ids, expectedValue)
	return t > 0 && d == t
}

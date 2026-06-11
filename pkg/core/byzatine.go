package core

import "math/rand"

// ByzantineAdversary controls the behavior of Byzantine nodes.
// When a Byzantine node attempts to send a message, the simulator calls
// InterceptSend instead of delivering the original action. The adversary
// can modify, drop, duplicate, or forge messages.
//
// This design keeps algorithms clean: they describe correct behavior only.
// Byzantine behavior is injected externally by the simulator.
type ByzantineAdversary interface {
	// InterceptSend is called when a Byzantine node produces a send action.
	// It receives the sender ID, the original action, the list of all
	// neighbors, and a seeded RNG for deterministic behavior.
	// It returns the list of actions to actually execute. This may be:
	//   - empty (drop the message)
	//   - the original action unchanged (behave correctly)
	//   - modified actions (corrupt payload, change target)
	//   - multiple actions (equivocation: send different payloads to different nodes)
	InterceptSend(from NodeID, action Action, neighbors []NodeID, rng *rand.Rand) []Action
}

// ByzantineConfig configures Byzantine fault injection.
type ByzantineConfig struct {
	// ByzantineNodeRate is the fraction of nodes to mark as Byzantine.
	// e.g. 0.33 means ~1/3 of nodes will be Byzantine.
	ByzantineNodeRate float64

	// Adversary controls how Byzantine nodes behave.
	Adversary ByzantineAdversary

	// FixedByzantineNodes, if non-empty, overrides ByzantineNodeRate
	// and marks exactly these nodes as Byzantine.
	FixedByzantineNodes []NodeID
}

// ── Built-in adversaries ─────────────────────────────────────────────────────

// SilentAdversary drops all messages from Byzantine nodes.
// The node appears to have crashed from the perspective of other nodes,
// but it still receives messages and processes events internally.
type SilentAdversary struct{}

func (SilentAdversary) InterceptSend(_ NodeID, _ Action, _ []NodeID, _ *rand.Rand) []Action {
	return nil // drop everything
}

// CorruptingAdversary replaces the payload of every message with a
// fixed corrupted value.
type CorruptingAdversary struct {
	CorruptedValue any
}

func (c CorruptingAdversary) InterceptSend(_ NodeID, action Action, _ []NodeID, _ *rand.Rand) []Action {
	action.Payload = c.CorruptedValue
	return []Action{action}
}

// EquivocatingAdversary sends the correct message to some neighbors and
// a corrupted message to others. This is the classic Byzantine behavior:
// telling different things to different nodes.
type EquivocatingAdversary struct {
	// CorruptedValue is sent to a random subset of neighbors.
	CorruptedValue any
	// CorruptFraction is the fraction of neighbors that receive the corrupted value.
	CorruptFraction float64
}

func (e EquivocatingAdversary) InterceptSend(_ NodeID, action Action, neighbors []NodeID, rng *rand.Rand) []Action {
	// Send correct value to the original target.
	// Additionally, with CorruptFraction probability, corrupt the payload.
	if rng.Float64() < e.CorruptFraction {
		action.Payload = e.CorruptedValue
	}
	return []Action{action}
}

// ForgeAndFloodAdversary sends forged messages to ALL neighbors,
// not just the intended target. This simulates a malicious node trying
// to flood the network with false information.
type ForgeAndFloodAdversary struct {
	ForgedValue any
}

func (f ForgeAndFloodAdversary) InterceptSend(_ NodeID, _ Action, neighbors []NodeID, _ *rand.Rand) []Action {
	actions := make([]Action, 0, len(neighbors))
	for _, nb := range neighbors {
		actions = append(actions, Action{
			Type:    ActionSend,
			SendTo:  nb,
			Payload: f.ForgedValue,
		})
	}
	return actions
}

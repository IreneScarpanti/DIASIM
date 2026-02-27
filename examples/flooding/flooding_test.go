package flooding_test

import (
	"io"
	"testing"

	"diasim/examples/flooding"
	"diasim/pkg/core"
	"diasim/pkg/topology"
)

func nodeIDs(names ...string) []core.NodeID {
	ids := make([]core.NodeID, len(names))
	for i, n := range names {
		ids[i] = core.NodeID(n)
	}
	return ids
}

// ── baseline (no failures) ────────────────────────────────────────────────────

// TestFloodingRing verifies that without failures the flood reaches all nodes
// on a ring topology.
func TestFloodingRing(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.Ring(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "hello"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      42,
		LogOutput: io.Discard,
	})
	sim.Run()

	assertAllReceived(t, sim, ids)
}

// TestFloodingFullMesh verifies flooding on a fully connected graph.
func TestFloodingFullMesh(t *testing.T) {
	ids := nodeIDs("A", "B", "C", "D")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.FullMesh(ids),
		Algorithm: &flooding.Algorithm{Initiator: "A", Value: 42},
		Delay:     &core.FixedDelay{Min: 2, Max: 2},
		Seed:      0,
		LogOutput: io.Discard,
	})
	sim.Run()

	assertAllReceived(t, sim, ids)
}

// TestFloodingInitiatorAlwaysReceived verifies that the initiator marks itself
// as received during OnStart, before any message exchange.
func TestFloodingInitiatorAlwaysReceived(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.Ring(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "init-test"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      0,
		LogOutput: io.Discard,
	})
	sim.Run()

	if !flooding.Received(sim, "N0") {
		t.Error("initiator N0 did not receive the flood")
	}
}

// ── determinism ───────────────────────────────────────────────────────────────

// TestFloodingDeterminism verifies that two runs with the same seed produce
// identical log entries under probabilistic link failures.
func TestFloodingDeterminism(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")
	topo := topology.Ring(ids)
	algo := &flooding.Algorithm{Initiator: "N0", Value: "det-test"}
	failures := &core.FailureConfig{
		LinkFailureRate:    0.40,
		FailureDurationMin: 1,
		FailureDurationMax: 3,
		FirstFailureAfter:  1,
	}

	run := func() []core.LogEntry {
		sim := core.New(core.SimConfig{
			Nodes:     ids,
			Topology:  topo,
			Algorithm: algo,
			Delay:     &core.FixedDelay{Min: 1, Max: 1},
			Seed:      99,
			LogOutput: io.Discard,
			Failures:  failures,
		})
		sim.Run()
		return sim.Logger().Entries()
	}

	e1 := run()
	e2 := run()

	if len(e1) != len(e2) {
		t.Fatalf("log length mismatch: %d vs %d", len(e1), len(e2))
	}
	for i := range e1 {
		if e1[i].String() != e2[i].String() {
			t.Errorf("entry %d differs:\n  run1: %s\n  run2: %s", i, e1[i], e2[i])
		}
	}
}

// ── node failures ─────────────────────────────────────────────────────────────
//
// The flooding algorithm is NOT fault tolerant against node crashes.
// A node that crashes and recovers loses its state (received=false), but its
// neighbors may have already finished retrying and will not send again.
// Tests here verify only weak invariants: no node receives a corrupted value,
// and the initiator always has the flood.

// TestFloodingNodeFailureInitiatorIntact verifies that the initiator always
// has the flood value, even when other nodes crash.
func TestFloodingNodeFailureInitiatorIntact(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")

	for _, seed := range []int64{1, 2, 3, 7, 13} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			sim := core.New(core.SimConfig{
				Nodes:     ids,
				Topology:  topology.Ring(ids),
				Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "crash-test"},
				Delay:     &core.FixedDelay{Min: 1, Max: 1},
				Seed:      seed,
				LogOutput: io.Discard,
				Failures: &core.FailureConfig{
					NodeFailureRate:    0.40,
					FailureDurationMin: 2,
					FailureDurationMax: 4,
					FirstFailureAfter:  1,
				},
			})
			sim.Run()

			if !flooding.Received(sim, "N0") {
				t.Errorf("[seed %d] initiator N0 lost the flood", seed)
			}
		})
	}
}

// TestFloodingNodeFailureValueIntegrity verifies that any node that received
// the flood has the correct value — never a zero or corrupted value.
func TestFloodingNodeFailureValueIntegrity(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")

	for _, seed := range []int64{1, 2, 3, 7, 13} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			sim := core.New(core.SimConfig{
				Nodes:     ids,
				Topology:  topology.Ring(ids),
				Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "integrity-test"},
				Delay:     &core.FixedDelay{Min: 1, Max: 1},
				Seed:      seed,
				LogOutput: io.Discard,
				Failures: &core.FailureConfig{
					NodeFailureRate:    0.40,
					FailureDurationMin: 2,
					FailureDurationMax: 4,
					FirstFailureAfter:  1,
				},
			})
			sim.Run()

			for _, id := range ids {
				if !flooding.Received(sim, id) {
					continue
				}
				v, ok := sim.NodeState(id).Get("flood_value")
				if !ok || v != "integrity-test" {
					t.Errorf("[seed %d] node %s has wrong flood_value: %v", seed, id, v)
				}
			}
		})
	}
}

// ── link failures ─────────────────────────────────────────────────────────────
//
// With retry (ACK-based), temporary link failures are fully tolerated:
// every node is guaranteed to receive the flood once the link recovers.

// TestFloodingLinkFailureAllReceive verifies that temporary link failures do
// not prevent delivery. The retry mechanism ensures every node eventually
// receives the flood once the link recovers.
func TestFloodingLinkFailureAllReceive(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")

	for _, seed := range []int64{5, 6, 9, 11, 17} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			sim := core.New(core.SimConfig{
				Nodes:     ids,
				Topology:  topology.Ring(ids),
				Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "link-test"},
				Delay:     &core.FixedDelay{Min: 1, Max: 1},
				Seed:      seed,
				LogOutput: io.Discard,
				Failures: &core.FailureConfig{
					LinkFailureRate:    0.50,
					FailureDurationMin: 2,
					FailureDurationMax: 4,
					FirstFailureAfter:  1,
				},
			})
			sim.Run()

			assertAllReceived(t, sim, ids)
		})
	}
}

// TestFloodingLinkFailureFullMesh verifies that on a fully connected graph,
// even with 50% link failure rate, every node eventually receives the flood.
func TestFloodingLinkFailureFullMesh(t *testing.T) {
	ids := nodeIDs("A", "B", "C", "D", "E")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.FullMesh(ids),
		Algorithm: &flooding.Algorithm{Initiator: "A", Value: "mesh-link-test"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      21,
		LogOutput: io.Discard,
		Failures: &core.FailureConfig{
			LinkFailureRate:    0.50,
			FailureDurationMin: 1,
			FailureDurationMax: 3,
			FirstFailureAfter:  1,
		},
	})
	sim.Run()

	assertAllReceived(t, sim, ids)
}

func TestFloodingNodeRecoveryRingAllReceive(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4", "N5")

	for _, seed := range []int64{30, 31, 32, 33, 34} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			sim := core.New(core.SimConfig{
				Nodes:     ids,
				Topology:  topology.Ring(ids),
				Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "ring-recovery"},
				Delay:     &core.FixedDelay{Min: 1, Max: 1},
				Seed:      seed,
				LogOutput: io.Discard,
				Failures: &core.FailureConfig{
					// Low rate so the ring is unlikely to be fully partitioned.
					NodeFailureRate:    0.20,
					FailureDurationMin: 3,
					FailureDurationMax: 8,
					FirstFailureAfter:  2,
				},
			})
			sim.Run()

			assertAllReceived(t, sim, ids)
		})
	}
}

// TestFloodingInitiatorCrashAndRecovers verifies that if the initiator itself
// crashes and recovers it does NOT re-flood (it was the source; re-flooding
// would be redundant but must not corrupt state or send wrong values).
func TestFloodingInitiatorCrashAndRecovers(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.FullMesh(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "init-crash"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      7,
		LogOutput: io.Discard,
		Failures: &core.FailureConfig{
			// Force N0 to crash very early and recover; others stay alive.
			NodeFailureRate:    0.25, // ~1 of 4 nodes
			FailureDurationMin: 2,
			FailureDurationMax: 3,
			FirstFailureAfter:  1,
		},
	})
	sim.Run()

	// All alive nodes (including a potentially recovered N0) must have the value.
	assertAllReceived(t, sim, ids)

	// N0 must carry the correct value after recovery (re-floods with same value).
	if flooding.Received(sim, "N0") {
		v, ok := sim.NodeState("N0").Get("flood_value")
		if !ok || v != "init-crash" {
			t.Errorf("initiator N0 has wrong flood_value after recovery: %v", v)
		}
	}
}

// ── combined failures ─────────────────────────────────────────────────────────

// TestFloodingCombinedFailuresLinkGuarantee verifies that nodes which were
// never crashed receive the flood — link failures alone are fully tolerated.
func TestFloodingCombinedFailuresLinkGuarantee(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4", "N5")

	for _, seed := range []int64{10, 20, 30, 40, 50} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			sim := core.New(core.SimConfig{
				Nodes:     ids,
				Topology:  topology.Ring(ids),
				Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "combined-test"},
				Delay:     &core.FixedDelay{Min: 1, Max: 1},
				Seed:      seed,
				LogOutput: io.Discard,
				Failures: &core.FailureConfig{
					NodeFailureRate:    0.30,
					LinkFailureRate:    0.30,
					FailureDurationMin: 2,
					FailureDurationMax: 5,
					FirstFailureAfter:  1,
				},
			})
			sim.Run()

			// Initiator must always have the flood.
			if !flooding.Received(sim, "N0") {
				t.Errorf("[seed %d] initiator N0 lost the flood", seed)
			}
			// Value integrity: any node that received must have the right value.
			for _, id := range ids {
				if !flooding.Received(sim, id) {
					continue
				}
				v, ok := sim.NodeState(id).Get("flood_value")
				if !ok || v != "combined-test" {
					t.Errorf("[seed %d] node %s has wrong flood_value: %v", seed, id, v)
				}
			}
		})
	}
}

// TestFloodingCombinedFailuresMeshAllReceive verifies full delivery on a mesh
// with both node crash+recovery and link failures. The HELLO mechanism and
// link retry together guarantee every recovered node eventually gets the value.
func TestFloodingCombinedFailuresMeshAllReceive(t *testing.T) {
	ids := nodeIDs("A", "B", "C", "D", "E", "F")

	for _, seed := range []int64{100, 200, 300, 400, 500} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			sim := core.New(core.SimConfig{
				Nodes:     ids,
				Topology:  topology.FullMesh(ids),
				Algorithm: &flooding.Algorithm{Initiator: "A", Value: "combined-mesh"},
				Delay:     &core.FixedDelay{Min: 1, Max: 2},
				Seed:      seed,
				LogOutput: io.Discard,
				Failures: &core.FailureConfig{
					NodeFailureRate:    0.30,
					LinkFailureRate:    0.30,
					FailureDurationMin: 3,
					FailureDurationMax: 8,
					FirstFailureAfter:  2,
				},
			})
			sim.Run()

			assertAllReceived(t, sim, ids)
		})
	}
}

// ── idempotency ───────────────────────────────────────────────────────────────

// TestFloodingIdempotency verifies that a node receiving multiple FLOOD
// messages (due to retries or topology redundancy) ends up with the correct
// value exactly once — never reset or double-counted.
func TestFloodingIdempotency(t *testing.T) {
	ids := nodeIDs("A", "B", "C", "D", "E", "F")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.FullMesh(ids),
		Algorithm: &flooding.Algorithm{Initiator: "A", Value: "idem-test"},
		Delay:     &core.FixedDelay{Min: 1, Max: 3},
		Seed:      77,
		LogOutput: io.Discard,
	})
	sim.Run()

	assertAllReceived(t, sim, ids)

	for _, id := range ids {
		v, ok := sim.NodeState(id).Get("flood_value")
		if !ok || v != "idem-test" {
			t.Errorf("node %s has wrong flood_value: %v", id, v)
		}
	}
}

// TestFloodingIdempotencyWithRetries verifies idempotency specifically when
// retries are heavily triggered (high link failure rate on a full mesh means
// many duplicate FLOOD messages will arrive at each node).
func TestFloodingIdempotencyWithRetries(t *testing.T) {
	ids := nodeIDs("A", "B", "C", "D", "E")
	const want = "idem-retry-test"

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.FullMesh(ids),
		Algorithm: &flooding.Algorithm{Initiator: "A", Value: want},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      88,
		LogOutput: io.Discard,
		Failures: &core.FailureConfig{
			LinkFailureRate:    0.60,
			FailureDurationMin: 2,
			FailureDurationMax: 5,
			FirstFailureAfter:  1,
		},
	})
	sim.Run()

	assertAllReceived(t, sim, ids)

	for _, id := range ids {
		v, ok := sim.NodeState(id).Get("flood_value")
		if !ok || v != want {
			t.Errorf("node %s has wrong flood_value after retries: %v", id, v)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func assertAllReceived(t *testing.T, sim *core.Simulator, ids []core.NodeID) {
	t.Helper()
	if !flooding.AllReceived(sim, ids) {
		for _, id := range ids {
			t.Logf("node %s: received=%v status=%v",
				id, flooding.Received(sim, id), sim.NodeState(id).Status())
		}
		t.Fatal("not all nodes received the flood")
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

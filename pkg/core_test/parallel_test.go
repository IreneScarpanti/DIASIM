package core_test

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

// TestParallelFloodingRing verifies that the parallel CMB engine correctly
// executes flooding on a ring topology without failures.
func TestParallelFloodingRing(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.Ring(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "parallel-ring"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      42,
		LogOutput: io.Discard,
		Mode:      core.ModeParallel,
	})
	sim.Run()

	assertAllReceived(t, sim, ids)
}

// TestParallelFloodingFullMesh verifies parallel flooding on a fully
// connected graph.
func TestParallelFloodingFullMesh(t *testing.T) {
	ids := nodeIDs("A", "B", "C", "D")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.FullMesh(ids),
		Algorithm: &flooding.Algorithm{Initiator: "A", Value: 42},
		Delay:     &core.FixedDelay{Min: 2, Max: 2},
		Seed:      0,
		LogOutput: io.Discard,
		Mode:      core.ModeParallel,
	})
	sim.Run()

	assertAllReceived(t, sim, ids)
}

// TestParallelFloodingLargerRing verifies parallel flooding on a larger ring.
func TestParallelFloodingLargerRing(t *testing.T) {
	names := make([]string, 20)
	for i := range names {
		names[i] = "N" + itoa(int64(i))
	}
	ids := nodeIDs(names...)

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.Ring(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "big-ring"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      7,
		LogOutput: io.Discard,
		Mode:      core.ModeParallel,
	})
	sim.Run()

	assertAllReceived(t, sim, ids)
}

// TestParallelFloodingPerLinkDelay verifies parallel execution with per-link
// delay model.
func TestParallelFloodingPerLinkDelay(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.Ring(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "perlink"},
		Delay:     &core.PerLinkDelay{Min: 1, Max: 3},
		Seed:      42,
		LogOutput: io.Discard,
		Mode:      core.ModeParallel,
	})
	sim.Run()

	assertAllReceived(t, sim, ids)
}

// TestParallelFloodingSeededDelay verifies parallel execution with seeded
// random delay model.
func TestParallelFloodingSeededDelay(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")

	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.Ring(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "seeded"},
		Delay:     &core.SeededDelay{Min: 1, Max: 5},
		Seed:      42,
		LogOutput: io.Discard,
		Mode:      core.ModeParallel,
	})
	sim.Run()

	assertAllReceived(t, sim, ids)
}

// TestParallelFloodingLinkFailures verifies that the parallel engine
// correctly handles temporary link failures.
func TestParallelFloodingLinkFailures(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")

	for _, seed := range []int64{5, 6, 9, 11, 17} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			sim := core.New(core.SimConfig{
				Nodes:     ids,
				Topology:  topology.Ring(ids),
				Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "link-fail"},
				Delay:     &core.FixedDelay{Min: 1, Max: 1},
				Seed:      seed,
				LogOutput: io.Discard,
				Mode:      core.ModeParallel,
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

// TestParallelFloodingNodeFailures verifies that the parallel engine
// correctly handles node crashes with recovery.
func TestParallelFloodingNodeFailures(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")

	for _, seed := range []int64{1, 2, 3, 7, 13} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			sim := core.New(core.SimConfig{
				Nodes:     ids,
				Topology:  topology.Ring(ids),
				Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "node-fail"},
				Delay:     &core.FixedDelay{Min: 1, Max: 1},
				Seed:      seed,
				LogOutput: io.Discard,
				Mode:      core.ModeParallel,
				Failures: &core.FailureConfig{
					NodeFailureRate:    0.40,
					FailureDurationMin: 2,
					FailureDurationMax: 4,
					FirstFailureAfter:  1,
				},
			})
			sim.Run()

			// Initiator must always have the flood.
			if !flooding.Received(sim, "N0") {
				t.Errorf("[seed %d] initiator N0 lost the flood", seed)
			}
		})
	}
}

// TestParallelFloodingCombinedFailures verifies parallel execution with
// both node crashes and link failures on a full mesh.
func TestParallelFloodingCombinedFailures(t *testing.T) {
	ids := nodeIDs("A", "B", "C", "D", "E", "F")

	for _, seed := range []int64{100, 200, 300, 400, 500} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			sim := core.New(core.SimConfig{
				Nodes:     ids,
				Topology:  topology.FullMesh(ids),
				Algorithm: &flooding.Algorithm{Initiator: "A", Value: "combined-parallel"},
				Delay:     &core.FixedDelay{Min: 1, Max: 2},
				Seed:      seed,
				LogOutput: io.Discard,
				Mode:      core.ModeParallel,
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

// ── Sequential vs parallel equivalence ───────────────────────────────────────

// TestParallelSequentialEquivalence verifies that the parallel engine
// produces the same final algorithm state as the sequential engine.
// The log traces may differ (as per CMB semantics), but the algorithm
// result (which nodes received the flood) must be identical.
func TestParallelSequentialEquivalence(t *testing.T) {
	ids := nodeIDs("N0", "N1", "N2", "N3", "N4", "N5")

	for _, seed := range []int64{1, 42, 99, 123, 777} {
		seed := seed
		t.Run("seed"+itoa(seed), func(t *testing.T) {
			cfg := core.SimConfig{
				Nodes:     ids,
				Topology:  topology.Ring(ids),
				Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "equiv-test"},
				Delay:     &core.FixedDelay{Min: 1, Max: 1},
				Seed:      seed,
				LogOutput: io.Discard,
			}

			// Run sequential.
			cfg.Mode = core.ModeSequential
			simSeq := core.New(cfg)
			simSeq.Run()

			// Run parallel.
			cfg.Mode = core.ModeParallel
			simPar := core.New(cfg)
			simPar.Run()

			// Compare results: every node must have the same received status.
			for _, id := range ids {
				seqRecv := flooding.Received(simSeq, id)
				parRecv := flooding.Received(simPar, id)
				if seqRecv != parRecv {
					t.Errorf("[seed %d] node %s: sequential=%v parallel=%v",
						seed, id, seqRecv, parRecv)
				}
			}
		})
	}
}

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
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

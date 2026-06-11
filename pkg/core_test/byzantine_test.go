package core

import (
	"fmt"
	"io"
	"testing"

	"diasim/examples/bracha"
	"diasim/examples/flooding"
	"diasim/examples/flooding_naive"
	"diasim/pkg/core"
	"diasim/pkg/topology"
)

func makeIDs(n int) []core.NodeID {
	ids := make([]core.NodeID, n)
	for i := range ids {
		ids[i] = core.NodeID(fmt.Sprintf("N%d", i))
	}
	return ids
}

// ── Bracha: no failures (baseline) ──────────────────────────────────────────

func TestBrachaNoFailures(t *testing.T) {
	ids := makeIDs(7)
	sim := core.New(core.SimConfig{
		Nodes: ids, Topology: topology.FullMesh(ids),
		Algorithm: &bracha.Algorithm{Broadcaster: "N0", Value: "hello", F: 2},
		Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: 42,
		LogOutput: io.Discard,
	})
	sim.Run()

	if !bracha.AllCorrectDelivered(sim, ids, "hello") {
		for _, id := range ids {
			t.Logf("  %s: delivered=%v", id, bracha.Delivered(sim, id))
		}
		t.Fatal("not all nodes delivered")
	}
}

// ── Bracha: Byzantine with silent adversary (up to f < n/3) ─────────────────

func TestBrachaByzantineSilent(t *testing.T) {
	for _, n := range []int{4, 7, 10, 13} {
		f := (n - 1) / 3 // max Byzantine faults
		t.Run(fmt.Sprintf("n=%d_f=%d", n, f), func(t *testing.T) {
			ids := makeIDs(n)
			// Make the last f nodes Byzantine (never the broadcaster N0).
			byzNodes := make([]core.NodeID, f)
			for i := 0; i < f; i++ {
				byzNodes[i] = ids[n-1-i]
			}

			sim := core.New(core.SimConfig{
				Nodes: ids, Topology: topology.FullMesh(ids),
				Algorithm: &bracha.Algorithm{Broadcaster: "N0", Value: "secure", F: f},
				Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: 42,
				LogOutput: io.Discard,
				Byzantine: &core.ByzantineConfig{
					FixedByzantineNodes: byzNodes,
					Adversary:           core.SilentAdversary{},
				},
			})
			sim.Run()

			d, total := bracha.CountCorrectDeliveries(sim, ids, "secure")
			if d != total {
				t.Errorf("n=%d f=%d: only %d/%d correct nodes delivered", n, f, d, total)
			}
		})
	}
}

// ── Bracha: Byzantine with corrupting adversary ─────────────────────────────

func TestBrachaByzantineCorrupting(t *testing.T) {
	for _, n := range []int{7, 10, 13} {
		f := (n - 1) / 3
		t.Run(fmt.Sprintf("n=%d_f=%d", n, f), func(t *testing.T) {
			ids := makeIDs(n)
			byzNodes := make([]core.NodeID, f)
			for i := 0; i < f; i++ {
				byzNodes[i] = ids[n-1-i]
			}

			sim := core.New(core.SimConfig{
				Nodes: ids, Topology: topology.FullMesh(ids),
				Algorithm: &bracha.Algorithm{Broadcaster: "N0", Value: "truth", F: f},
				Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: 42,
				LogOutput: io.Discard,
				Byzantine: &core.ByzantineConfig{
					FixedByzantineNodes: byzNodes,
					Adversary:           core.CorruptingAdversary{CorruptedValue: bracha.Msg{Kind: "ECHO", Value: "lies"}},
				},
			})
			sim.Run()

			d, total := bracha.CountCorrectDeliveries(sim, ids, "truth")
			if d != total {
				t.Errorf("n=%d f=%d: only %d/%d correct nodes delivered truth", n, f, d, total)
			}

			// Verify no correct node delivered "lies".
			dLies, _ := bracha.CountCorrectDeliveries(sim, ids, "lies")
			if dLies > 0 {
				t.Errorf("n=%d f=%d: %d correct nodes delivered corrupted value!", n, f, dLies)
			}
		})
	}
}

// ── Flooding naive: fails under crash ───────────────────────────────────────

func TestFloodingNaiveFailsUnderCrash(t *testing.T) {
	ids := makeIDs(10)
	sim := core.New(core.SimConfig{
		Nodes: ids, Topology: topology.FullMesh(ids),
		Algorithm: &flooding_naive.Algorithm{Initiator: "N0", Value: "data"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: 42,
		LogOutput: io.Discard,
		Failures: &core.FailureConfig{
			NodeFailureRate:    0.30,
			FailureDurationMin: 100, // effectively permanent
			FailureDurationMax: 100,
			FirstFailureAfter:  1,
		},
	})
	sim.Run()

	// With no retry mechanism, some nodes may not receive the value.
	// This is expected — naive flooding is not fault-tolerant.
	d, total := flooding_naive.CountCorrectDeliveries(sim, ids, "data")
	t.Logf("naive flooding under crash: %d/%d correct nodes received", d, total)
}

// ── Flooding naive: fails under Byzantine ───────────────────────────────────

func TestFloodingNaiveFailsUnderByzantine(t *testing.T) {
	ids := makeIDs(10)
	sim := core.New(core.SimConfig{
		Nodes: ids, Topology: topology.FullMesh(ids),
		Algorithm: &flooding_naive.Algorithm{Initiator: "N0", Value: "real"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: 42,
		LogOutput: io.Discard,
		Byzantine: &core.ByzantineConfig{
			FixedByzantineNodes: []core.NodeID{"N1", "N2", "N3"},
			Adversary:           core.CorruptingAdversary{CorruptedValue: flooding_naive.Msg{Value: "fake"}},
		},
	})
	sim.Run()

	// Byzantine nodes forward corrupted values. Naive flooding accepts them.
	d, total := flooding_naive.CountCorrectDeliveries(sim, ids, "real")
	t.Logf("naive flooding under byzantine: %d/%d correct nodes got real value", d, total)
	// Some correct nodes may have received "fake" instead of "real".
	dFake, _ := flooding_naive.CountCorrectDeliveries(sim, ids, "fake")
	t.Logf("naive flooding under byzantine: %d correct nodes got fake value", dFake)
}

// ── Flooding (with ACK): survives crash, fails under Byzantine ──────────────

func TestFloodingWithACKSurvivesCrash(t *testing.T) {
	ids := makeIDs(10)
	sim := core.New(core.SimConfig{
		Nodes: ids, Topology: topology.FullMesh(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "data"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: 42,
		LogOutput: io.Discard,
		Failures: &core.FailureConfig{
			NodeFailureRate:    0.20,
			FailureDurationMin: 5,
			FailureDurationMax: 10,
			FirstFailureAfter:  1,
		},
	})
	sim.Run()

	// Flooding with ACK and retry should deliver to all recovered nodes.
	d, total := flooding.CountCorrectDeliveries(sim, ids, "data")
	t.Logf("flooding with ACK under crash: %d/%d correct nodes received", d, total)
	if d != total {
		t.Errorf("expected all %d correct nodes to receive, got %d", total, d)
	}
}

// NOTE: TestFloodingWithACKFailsUnderByzantine is intentionally not included.
// Flooding with ACK retries forever waiting for ACKs from Byzantine (silent)
// nodes, causing the simulation to never terminate. This is itself a failure
// mode: the algorithm is not Byzantine-fault-tolerant, and a Byzantine node
// can cause the initiator to retry indefinitely. This is a key motivation
// for using Bracha's algorithm instead.

// ── Bracha: resilience scaling (f from 0 to n/3) ────────────────────────────

func TestBrachaResilienceScaling(t *testing.T) {
	n := 31 // large enough to test several f values
	ids := makeIDs(n)
	maxF := (n - 1) / 3 // = 10

	for f := 0; f <= maxF; f++ {
		t.Run(fmt.Sprintf("f=%d", f), func(t *testing.T) {
			byzNodes := make([]core.NodeID, f)
			for i := 0; i < f; i++ {
				byzNodes[i] = ids[n-1-i]
			}

			sim := core.New(core.SimConfig{
				Nodes: ids, Topology: topology.FullMesh(ids),
				Algorithm: &bracha.Algorithm{Broadcaster: "N0", Value: "scale", F: maxF},
				Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: 42,
				LogOutput: io.Discard,
				Byzantine: &core.ByzantineConfig{
					FixedByzantineNodes: byzNodes,
					Adversary:           core.SilentAdversary{},
				},
			})
			sim.Run()

			d, total := bracha.CountCorrectDeliveries(sim, ids, "scale")
			if d != total {
				t.Errorf("f=%d: only %d/%d correct nodes delivered", f, d, total)
			}
		})
	}
}

// ── Bracha: breaks when f exceeds n/3 ───────────────────────────────────────

func TestBrachaBreaksOverThreshold(t *testing.T) {
	n := 10
	ids := makeIDs(n)
	f := (n - 1) / 3 // 3

	// Make f+1 = 4 nodes Byzantine (exceeds threshold).
	byzNodes := make([]core.NodeID, f+1)
	for i := 0; i <= f; i++ {
		byzNodes[i] = ids[n-1-i]
	}

	sim := core.New(core.SimConfig{
		Nodes: ids, Topology: topology.FullMesh(ids),
		Algorithm: &bracha.Algorithm{Broadcaster: "N0", Value: "broken", F: f},
		Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: 42,
		LogOutput: io.Discard,
		Byzantine: &core.ByzantineConfig{
			FixedByzantineNodes: byzNodes,
			Adversary:           core.SilentAdversary{},
		},
	})
	sim.Run()

	d, total := bracha.CountCorrectDeliveries(sim, ids, "broken")
	t.Logf("bracha over threshold: %d/%d correct nodes delivered (f+1=%d Byzantine, n=%d)",
		d, total, f+1, n)
	// With f+1 silent Byzantine nodes, some correct nodes may fail to deliver.
	// This is expected — the algorithm is only guaranteed for f < n/3.
}

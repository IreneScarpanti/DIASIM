package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"diasim/examples/flooding"
	"diasim/pkg/core"
	"diasim/pkg/topology"
)

func main() {
	demoFlooding()
	demoFloodingWithNodeFailures()
	demoFloodingWithLinkFailures()
	demoFloodingWithBothFailures()
	demoBatchComparison()
}

func demoFlooding() {
	printHeader("DEMO 1 — Flooding on a 6-node ring (no failures)")

	ids := nodeIDs("N0", "N1", "N2", "N3", "N4", "N5")
	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.FullMesh(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "hello-world"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      42,
		LogOutput: os.Stdout,
		LogLevel:  core.LevelInfo,
	})
	sim.Run()
	printFloodResult(sim, ids)
}

func demoFloodingWithNodeFailures() {
	printHeader("DEMO 2 — Flooding: 40% node failure rate, recovery in [2, 4]")

	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")
	graph := topology.New()
	graph.AddBiEdge("N0", "N1")
	graph.AddBiEdge("N0", "N2")
	graph.AddBiEdge("N2", "N3")
	graph.AddBiEdge("N1", "N3")
	graph.AddBiEdge("N0", "N4")
	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  graph,
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "node-fail-demo"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      7,
		LogOutput: os.Stdout,
		LogLevel:  core.LevelInfo,
		Failures: &core.FailureConfig{
			NodeFailureRate:    0.40,
			FailureDurationMin: 2,
			FailureDurationMax: 4,
			FirstFailureAfter:  1,
		},
	})
	sim.Run()
	printFloodResult(sim, ids)
}

func demoFloodingWithLinkFailures() {
	printHeader("DEMO 3 — Flooding: 40% link failure rate, recovery in [1, 3]")

	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")
	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  topology.Ring(ids),
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "link-fail-demo"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1},
		Seed:      13,
		LogOutput: os.Stdout,
		LogLevel:  core.LevelInfo,
		Failures: &core.FailureConfig{
			LinkFailureRate:    0.40,
			FailureDurationMin: 1,
			FailureDurationMax: 3,
			FirstFailureAfter:  1,
		},
	})
	sim.Run()
	printFloodResult(sim, ids)
}

func demoFloodingWithBothFailures() {
	printHeader("DEMO 4 — Flooding: 20% node + 30% link failures, variable delay [1, 3]")

	ids := nodeIDs("N0", "N1", "N2", "N3", "N4")
	graph := topology.New()
	graph.AddBiEdge("N0", "N1")
	graph.AddBiEdge("N0", "N2")
	graph.AddBiEdge("N0", "N4")
	graph.AddBiEdge("N1", "N3")
	graph.AddBiEdge("N2", "N3")
	sim := core.New(core.SimConfig{
		Nodes:     ids,
		Topology:  graph,
		Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "both-fail-demo"},
		Delay:     &core.FixedDelay{Min: 1, Max: 3},
		Seed:      99,
		LogOutput: os.Stdout,
		LogLevel:  core.LevelInfo,
		Failures: &core.FailureConfig{
			NodeFailureRate:    0.20,
			LinkFailureRate:    0.30,
			FailureDurationMin: 2,
			FailureDurationMax: 5,
			FirstFailureAfter:  1,
		},
	})
	sim.Run()
	printFloodResult(sim, ids)
}

func demoBatchComparison() {
	printHeader("DEMO 5 — Determinism: BatchSize 1, 2, 4, 8 produce identical logs")

	ids := nodeIDs("N0", "N1", "N2", "N3", "N4", "N5")
	failures := &core.FailureConfig{
		NodeFailureRate:    0.40,
		LinkFailureRate:    0.20,
		FailureDurationMin: 1,
		FailureDurationMax: 3,
		FirstFailureAfter:  1,
	}

	batchSizes := []int{1, 2, 4, 8}
	logs := make([][]core.LogEntry, len(batchSizes))

	for i, bs := range batchSizes {
		sim := core.New(core.SimConfig{
			Nodes:     ids,
			Topology:  topology.Ring(ids),
			Algorithm: &flooding.Algorithm{Initiator: "N0", Value: "batch-test"},
			Delay:     &core.FixedDelay{Min: 1, Max: 1},
			Seed:      42,
			LogOutput: io.Discard,
			LogLevel:  core.LevelDebug,
			Failures:  failures,
			BatchSize: bs,
		})
		sim.Run()
		logs[i] = sim.Logger().Entries()
	}

	reference := logs[0]
	for i := 1; i < len(batchSizes); i++ {
		entries := logs[i]
		if len(entries) != len(reference) {
			fmt.Printf("  ✗ BatchSize=%d: %d entries vs %d\n", batchSizes[i], len(entries), len(reference))
			continue
		}
		mismatch := false
		for j := range reference {
			if entries[j].String() != reference[j].String() {
				fmt.Printf("  ✗ BatchSize=%d: entry %d differs\n", batchSizes[i], j)
				fmt.Printf("      BS=1: %s\n", reference[j])
				fmt.Printf("      BS=%d: %s\n", batchSizes[i], entries[j])
				mismatch = true
				break
			}
		}
		if !mismatch {
			fmt.Printf("  ✓ BatchSize=%d: log identical to BatchSize=1\n", batchSizes[i])
		}
	}
	fmt.Println()
}

// ── utilities ─────────────────────────────────────────────────────────────────

func nodeIDs(names ...string) []core.NodeID {
	ids := make([]core.NodeID, len(names))
	for i, n := range names {
		ids[i] = core.NodeID(n)
	}
	return ids
}

func printFloodResult(sim *core.Simulator, ids []core.NodeID) {
	fmt.Println()
	for _, id := range ids {
		n := sim.NodeState(id)
		switch {
		case n.Status() == core.StatusCrashed:
			fmt.Printf("  %s  ✗ crashed\n", id)
		case flooding.Received(sim, id):
			fmt.Printf("  %s  ✓\n", id)
		default:
			fmt.Printf("  %s  ✗ not received\n", id)
		}
	}
	fmt.Println()
}

func printHeader(title string) {
	line := strings.Repeat("─", len(title)+4)
	fmt.Printf("\n┌%s┐\n│  %s  │\n└%s┘\n\n", line, title, line)
}

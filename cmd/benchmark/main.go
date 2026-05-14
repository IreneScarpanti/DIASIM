package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	"diasim/examples/flooding"
	"diasim/examples/raft"
	"diasim/pkg/core"
	"diasim/pkg/topology"
)

func makeNodeIDs(n int) []core.NodeID {
	ids := make([]core.NodeID, n)
	for i := 0; i < n; i++ {
		ids[i] = core.NodeID(fmt.Sprintf("N%d", i))
	}
	return ids
}

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }

func runFloodingBenchmark(
	ids []core.NodeID, topo core.TopologyReader,
	mode core.ExecutionMode, seed int64,
) (wallTime time.Duration, steps int, ok bool) {
	sim := core.New(core.SimConfig{
		Nodes: ids, Topology: topo,
		Algorithm: &flooding.Algorithm{Initiator: ids[0], Value: "bench"},
		Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: seed,
		LogOutput: devNull{}, Mode: mode,
	})
	wallStart := time.Now()
	steps = sim.Run()
	wallTime = time.Since(wallStart)
	ok = flooding.AllReceived(sim, ids)
	return
}

func runRaftBenchmark(
	ids []core.NodeID, topo core.TopologyReader,
	mode core.ExecutionMode, seed int64, numValues int,
) (wallTime time.Duration, steps int, ok bool) {
	values := make([]any, numValues)
	for i := range values {
		values[i] = fmt.Sprintf("v%d", i)
	}
	sim := core.New(core.SimConfig{
		Nodes: ids, Topology: topo,
		Algorithm: &raft.Algorithm{Initiator: ids[0], Values: values},
		Delay:     &core.FixedDelay{Min: 1, Max: 1}, Seed: seed,
		LogOutput: devNull{}, Mode: mode,
	})
	wallStart := time.Now()
	steps = sim.Run()
	wallTime = time.Since(wallStart)
	ok = raft.AllCommitted(sim, ids, values)
	return
}

func writeHeader(w *csv.Writer) {
	w.Write([]string{
		"experiment", "algorithm", "topology", "nodes", "values",
		"mode", "seed", "repetition", "wall_time_us", "steps", "ok",
	})
}

func writeRow(w *csv.Writer, experiment, algo, topo string, nodes, values int,
	mode core.ExecutionMode, seed int64, rep int,
	wallTime time.Duration, steps int, ok bool) {
	w.Write([]string{
		experiment, algo, topo,
		strconv.Itoa(nodes), strconv.Itoa(values),
		string(mode), strconv.FormatInt(seed, 10), strconv.Itoa(rep),
		strconv.FormatInt(wallTime.Microseconds(), 10),
		strconv.Itoa(steps), strconv.FormatBool(ok),
	})
}

func main() {
	modes := []core.ExecutionMode{core.ModeSequential, core.ModeParallel}
	repetitions := 10
	seeds := make([]int64, repetitions)
	for i := range seeds {
		seeds[i] = int64(i*17 + 42)
	}

	outFile, err := os.Create("cmd/benchmark/benchmark_results.csv")
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot create output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	w := csv.NewWriter(outFile)
	defer w.Flush()
	writeHeader(w)

	numCPU := runtime.NumCPU()
	fmt.Printf("DIASIM Benchmark — %d CPUs available, GOMAXPROCS=%d\n", numCPU, runtime.GOMAXPROCS(0))
	fmt.Printf("Repetitions per config: %d\n\n", repetitions)

	// ── Experiment configs ────────────────────────────────────────────────

	// Exp 1: Flooding baseline (fullmesh, varying nodes)
	exp1Nodes := []int{10, 20, 50, 100, 500, 1000}

	// Exp 2: Raft varying nodes (fixed 100 values)
	exp2Nodes := []int{5, 7, 10, 20, 50, 100, 500, 1000}
	exp2Values := 100

	// Exp 3: Raft varying values (fixed 100 nodes)
	exp3Nodes := 100
	exp3Values := []int{10, 25, 50, 100, 200, 500, 1000}

	// ── Count total ──────────────────────────────────────────────────────
	total := (len(exp1Nodes) + len(exp2Nodes) + len(exp3Values)) * len(modes) * repetitions
	done := 0

	// ══════════════════════════════════════════════════════════════════════
	// EXPERIMENT 1: Flooding baseline
	// ══════════════════════════════════════════════════════════════════════
	fmt.Println("═══ Exp 1: Flooding (fullmesh, varying nodes) ═══")
	fmt.Printf("Node counts: %v\n\n", exp1Nodes)

	for _, n := range exp1Nodes {
		ids := makeNodeIDs(n)
		topo := topology.FullMesh(ids)
		for _, mode := range modes {
			for rep, seed := range seeds {
				wallTime, steps, ok := runFloodingBenchmark(ids, topo, mode, seed)
				writeRow(w, "flooding_nodes", "flooding", "fullmesh",
					n, 0, mode, seed, rep, wallTime, steps, ok)
				done++
				pct := float64(done) / float64(total) * 100
				fmt.Printf("\r  [%5.1f%%] flooding n=%-5d %-12s rep=%d  wall=%12s  steps=%-8d ok=%v",
					pct, n, mode, rep, wallTime.Round(time.Microsecond), steps, ok)
			}
			fmt.Println()
		}
	}

	// ══════════════════════════════════════════════════════════════════════
	// EXPERIMENT 2: Raft varying nodes (fixed values)
	// ══════════════════════════════════════════════════════════════════════
	fmt.Printf("\n═══ Exp 2: Raft (varying nodes, %d values) ═══\n", exp2Values)
	fmt.Printf("Node counts: %v\n\n", exp2Nodes)

	for _, n := range exp2Nodes {
		ids := makeNodeIDs(n)
		topo := topology.FullMesh(ids)
		for _, mode := range modes {
			for rep, seed := range seeds {
				wallTime, steps, ok := runRaftBenchmark(ids, topo, mode, seed, exp2Values)
				writeRow(w, "raft_nodes", "raft", "fullmesh",
					n, exp2Values, mode, seed, rep, wallTime, steps, ok)
				done++
				pct := float64(done) / float64(total) * 100
				fmt.Printf("\r  [%5.1f%%] raft     n=%-5d v=%-4d %-12s rep=%d  wall=%12s  steps=%-8d ok=%v",
					pct, n, exp2Values, mode, rep, wallTime.Round(time.Microsecond), steps, ok)
			}
			fmt.Println()
		}
	}

	// ══════════════════════════════════════════════════════════════════════
	// EXPERIMENT 3: Raft varying values (fixed nodes)
	// ══════════════════════════════════════════════════════════════════════
	fmt.Printf("\n═══ Exp 3: Raft (100 nodes, varying values) ═══\n")
	fmt.Printf("Value counts: %v\n\n", exp3Values)

	ids := makeNodeIDs(exp3Nodes)
	topo := topology.FullMesh(ids)

	for _, nv := range exp3Values {
		for _, mode := range modes {
			for rep, seed := range seeds {
				wallTime, steps, ok := runRaftBenchmark(ids, topo, mode, seed, nv)
				writeRow(w, "raft_values", "raft", "fullmesh",
					exp3Nodes, nv, mode, seed, rep, wallTime, steps, ok)
				done++
				pct := float64(done) / float64(total) * 100
				fmt.Printf("\r  [%5.1f%%] raft     n=%-5d v=%-4d %-12s rep=%d  wall=%12s  steps=%-8d ok=%v",
					pct, exp3Nodes, nv, mode, rep, wallTime.Round(time.Microsecond), steps, ok)
			}
			fmt.Println()
		}
	}

	fmt.Printf("\nResults written to benchmark_results.csv\n")
}

package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"strconv"

	"diasim/examples/bully"
	"diasim/examples/raft"
	"diasim/examples/raft_robust"
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

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }

// ── Result ───────────────────────────────────────────────────────────────────

type result struct {
	algorithm       string
	failureType     string
	n, f            int
	seed            int64
	leaderElected   bool
	leaderCorrect   bool
	leaderAgreement float64
	valuesCommitted float64
	valuesCorrect   float64
	steps           int
	terminated      bool
}

// ── Analysis helpers ─────────────────────────────────────────────────────────

func analyzeRaft(sim *core.Simulator, ids []core.NodeID, values []any) result {
	var r result
	leader := raft.LeaderID(sim, ids)
	r.leaderElected = leader != ""
	if leader != "" {
		ln := sim.NodeState(leader)
		r.leaderCorrect = ln != nil && !ln.IsByzantine()
	}
	leaderVotes := make(map[core.NodeID]int)
	correctCount := 0
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		correctCount++
		v, _ := n.Get("raft_currentLeader")
		if lid, ok := v.(core.NodeID); ok && lid != "" {
			leaderVotes[lid]++
		}
	}
	maxVotes := 0
	for _, c := range leaderVotes {
		if c > maxVotes {
			maxVotes = c
		}
	}
	if correctCount > 0 {
		r.leaderAgreement = float64(maxVotes) / float64(correctCount)
	}
	committed, correct := 0, 0
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		cv := raft.Committed(sim, id)
		if len(cv) >= len(values) {
			committed++
			match := true
			for i, v := range values {
				if i < len(cv) && cv[i] != v {
					match = false
					break
				}
			}
			if match {
				correct++
			}
		}
	}
	if correctCount > 0 {
		r.valuesCommitted = float64(committed) / float64(correctCount)
		r.valuesCorrect = float64(correct) / float64(correctCount)
	}
	return r
}

func analyzeRaftRobust(sim *core.Simulator, ids []core.NodeID, values []any) result {
	var r result
	leader := raft_robust.LeaderID(sim, ids)
	r.leaderElected = leader != ""
	if leader != "" {
		ln := sim.NodeState(leader)
		r.leaderCorrect = ln != nil && !ln.IsByzantine()
	}
	leaderVotes := make(map[core.NodeID]int)
	correctCount := 0
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		correctCount++
		v, _ := n.Get("raft_currentLeader")
		if lid, ok := v.(core.NodeID); ok && lid != "" {
			leaderVotes[lid]++
		}
	}
	maxVotes := 0
	for _, c := range leaderVotes {
		if c > maxVotes {
			maxVotes = c
		}
	}
	if correctCount > 0 {
		r.leaderAgreement = float64(maxVotes) / float64(correctCount)
	}
	committed, correct := 0, 0
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		cv := raft_robust.Committed(sim, id)
		if len(cv) >= len(values) {
			committed++
			match := true
			for i, v := range values {
				if i < len(cv) && cv[i] != v {
					match = false
					break
				}
			}
			if match {
				correct++
			}
		}
	}
	if correctCount > 0 {
		r.valuesCommitted = float64(committed) / float64(correctCount)
		r.valuesCorrect = float64(correct) / float64(correctCount)
	}
	return r
}

func analyzeBully(sim *core.Simulator, ids []core.NodeID, values []any) result {
	var r result
	leader := bully.LeaderID(sim, ids)
	r.leaderElected = leader != ""
	if leader != "" {
		ln := sim.NodeState(leader)
		r.leaderCorrect = ln != nil && !ln.IsByzantine()
	}
	agreedLeader, count, total := bully.AgreedLeader(sim, ids)
	_ = agreedLeader
	if total > 0 {
		r.leaderAgreement = float64(count) / float64(total)
	}
	committed, correct := 0, 0
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		cv := bully.Committed(sim, id)
		if len(cv) >= len(values) {
			committed++
			have := make(map[string]bool)
			for _, v := range cv {
				have[fmt.Sprintf("%v", v)] = true
			}
			allPresent := true
			for _, v := range values {
				if !have[fmt.Sprintf("%v", v)] {
					allPresent = false
					break
				}
			}
			if allPresent {
				correct++
			}
		}
	}
	if total > 0 {
		r.valuesCommitted = float64(committed) / float64(total)
		r.valuesCorrect = float64(correct) / float64(total)
	}
	return r
}

// ── Main ─────────────────────────────────────────────────────────────────────

func main() {
	n := 31
	maxF := 2 * n / 3 // 20
	reps := 10
	maxSteps := 50000
	seeds := make([]int64, reps)
	for i := range seeds {
		seeds[i] = int64(i*17 + 42)
	}

	ids := makeIDs(n)
	topo := topology.FullMesh(ids)
	values := []any{"v1", "v2", "v3"}
	delay := &core.SeededDelay{Min: 1, Max: 10}

	outFile, err := os.Create("comparison_results.csv")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	w := csv.NewWriter(outFile)
	defer w.Flush()

	w.Write([]string{
		"algorithm", "failure_type", "n", "f", "seed", "repetition",
		"leader_elected", "leader_correct", "leader_agreement",
		"values_committed", "values_correct",
		"steps", "terminated",
	})

	algos := []string{"bully", "raft", "raft_robust"}

	type scenario struct {
		name string
		desc string
	}
	scenarios := []scenario{
		{"crash_permanent", "f nodes crash permanently at t=1"},
		{"byz_active", "f Byzantine followers forge-and-flood"},
		{"byz_leader", "highest-priority node is Byzantine + f-1 followers"},
	}

	total := len(scenarios) * len(algos) * (maxF + 1) * reps
	done := 0

	fmt.Printf("DIASIM Comparison — n=%d, f=0..%d, %d CPUs, %d reps\n",
		n, maxF, runtime.NumCPU(), reps)
	fmt.Printf("Algorithms: %v | MaxSteps: %d\n\n", algos, maxSteps)

	for _, sc := range scenarios {
		fmt.Printf("═══ %s ═══\n%s\n\n", sc.name, sc.desc)

		for f := 0; f <= maxF; f++ {
			var faultyFollowers []core.NodeID
			if f > 0 {
				start := n - f
				if start < 1 {
					start = 1
				}
				faultyFollowers = ids[start:]
			}

			for _, algo := range algos {
				for rep, seed := range seeds {
					var r result

					switch sc.name {

					// ── Crash permanente ──────────────────────────────────────
					case "crash_permanent":
						crashes := make(map[core.NodeID]int64)
						for _, id := range faultyFollowers {
							crashes[id] = 1
						}
						switch algo {
						case "bully":
							sim := core.New(core.SimConfig{
								Nodes: ids, Topology: topo,
								Algorithm: &bully.Algorithm{Initiator: ids[0], Values: values},
								Delay:     delay, Seed: seed, LogOutput: devNull{},
								ScheduledCrashes: crashes, MaxSteps: maxSteps,
							})
							steps := sim.Run()
							r = analyzeBully(sim, ids, values)
							r.steps = steps
							r.terminated = steps < maxSteps
						case "raft":
							sim := core.New(core.SimConfig{
								Nodes: ids, Topology: topo,
								Algorithm: &raft.Algorithm{Initiator: ids[0], Values: values},
								Delay:     delay, Seed: seed, LogOutput: devNull{},
								ScheduledCrashes: crashes, MaxSteps: maxSteps,
							})
							steps := sim.Run()
							r = analyzeRaft(sim, ids, values)
							r.steps = steps
							r.terminated = steps < maxSteps
						case "raft_robust":
							sim := core.New(core.SimConfig{
								Nodes: ids, Topology: topo,
								Algorithm: &raft_robust.Algorithm{Initiator: ids[0], Values: values},
								Delay:     delay, Seed: seed, LogOutput: devNull{},
								ScheduledCrashes: crashes, MaxSteps: maxSteps,
							})
							steps := sim.Run()
							r = analyzeRaftRobust(sim, ids, values)
							r.steps = steps
							r.terminated = steps < maxSteps
						}

					// ── Byzantine attivo (forge-and-flood) ───────────────────
					case "byz_active":
						var byz *core.ByzantineConfig
						if f > 0 {
							switch algo {
							case "bully":
								forged := bully.Msg{Kind: "REPLICATE", From: "FAKE", Values: []any{"FORGED"}}
								byz = &core.ByzantineConfig{
									FixedByzantineNodes: faultyFollowers,
									Adversary:           core.ForgeAndFloodAdversary{ForgedValue: forged},
								}
							case "raft":
								forged := raft.Msg{Kind: "LOG_REQ", LeaderID: "FAKE", LTerm: 999}
								byz = &core.ByzantineConfig{
									FixedByzantineNodes: faultyFollowers,
									Adversary:           core.ForgeAndFloodAdversary{ForgedValue: forged},
								}
							case "raft_robust":
								// Stesso attacco: vogliamo verificare che il term bound
								// (modifica 1) blocchi lTerm=999 prima del processing.
								forged := raft_robust.Msg{Kind: "LOG_REQ", LeaderID: "FAKE", LTerm: 999}
								byz = &core.ByzantineConfig{
									FixedByzantineNodes: faultyFollowers,
									Adversary:           core.ForgeAndFloodAdversary{ForgedValue: forged},
								}
							}
						}
						switch algo {
						case "bully":
							sim := core.New(core.SimConfig{
								Nodes: ids, Topology: topo,
								Algorithm: &bully.Algorithm{Initiator: ids[0], Values: values},
								Delay:     delay, Seed: seed, LogOutput: devNull{},
								Byzantine: byz, MaxSteps: maxSteps,
							})
							steps := sim.Run()
							r = analyzeBully(sim, ids, values)
							r.steps = steps
							r.terminated = steps < maxSteps
						case "raft":
							sim := core.New(core.SimConfig{
								Nodes: ids, Topology: topo,
								Algorithm: &raft.Algorithm{Initiator: ids[0], Values: values},
								Delay:     delay, Seed: seed, LogOutput: devNull{},
								Byzantine: byz, MaxSteps: maxSteps,
							})
							steps := sim.Run()
							r = analyzeRaft(sim, ids, values)
							r.steps = steps
							r.terminated = steps < maxSteps
						case "raft_robust":
							sim := core.New(core.SimConfig{
								Nodes: ids, Topology: topo,
								Algorithm: &raft_robust.Algorithm{Initiator: ids[0], Values: values},
								Delay:     delay, Seed: seed, LogOutput: devNull{},
								Byzantine: byz, MaxSteps: maxSteps,
							})
							steps := sim.Run()
							r = analyzeRaftRobust(sim, ids, values)
							r.steps = steps
							r.terminated = steps < maxSteps
						}

					// ── Byzantine al leader ───────────────────────────────────
					case "byz_leader":
						if f >= 1 {
							byzNodes := []core.NodeID{ids[n-1]}
							if f > 1 {
								start := n - f
								if start < 1 {
									start = 1
								}
								for _, id := range ids[start : n-1] {
									byzNodes = append(byzNodes, id)
								}
							}
							switch algo {
							case "bully":
								adversary := core.EquivocatingAdversary{
									CorruptedValue:  bully.Msg{Kind: "REPLICATE", From: ids[n-1], Values: []any{"FAKE1", "FAKE2", "FAKE3"}},
									CorruptFraction: 0.5,
								}
								byz := &core.ByzantineConfig{FixedByzantineNodes: byzNodes, Adversary: adversary}
								sim := core.New(core.SimConfig{
									Nodes: ids, Topology: topo,
									Algorithm: &bully.Algorithm{Initiator: ids[0], Values: values},
									Delay:     delay, Seed: seed, LogOutput: devNull{},
									Byzantine: byz, MaxSteps: maxSteps,
								})
								steps := sim.Run()
								r = analyzeBully(sim, ids, values)
								r.steps = steps
								r.terminated = steps < maxSteps
							case "raft":
								adversary := core.EquivocatingAdversary{
									CorruptedValue:  raft.Msg{Kind: "LOG_REQ", LeaderID: ids[n-1], LTerm: 1, Suffix: []raft.LogEntry{{Term: 1, Value: "FAKE"}}},
									CorruptFraction: 0.5,
								}
								byz := &core.ByzantineConfig{FixedByzantineNodes: byzNodes, Adversary: adversary}
								sim := core.New(core.SimConfig{
									Nodes: ids, Topology: topo,
									Algorithm: &raft.Algorithm{Initiator: ids[0], Values: values},
									Delay:     delay, Seed: seed, LogOutput: devNull{},
									Byzantine: byz, MaxSteps: maxSteps,
								})
								steps := sim.Run()
								r = analyzeRaft(sim, ids, values)
								r.steps = steps
								r.terminated = steps < maxSteps
							case "raft_robust":
								// Stesso avversario equivocante: verifica se leader
								// lease (modifica 2) protegge il leader corretto.
								adversary := core.EquivocatingAdversary{
									CorruptedValue:  raft_robust.Msg{Kind: "LOG_REQ", LeaderID: ids[n-1], LTerm: 1, Suffix: []raft_robust.LogEntry{{Term: 1, Value: "FAKE"}}},
									CorruptFraction: 0.5,
								}
								byz := &core.ByzantineConfig{FixedByzantineNodes: byzNodes, Adversary: adversary}
								sim := core.New(core.SimConfig{
									Nodes: ids, Topology: topo,
									Algorithm: &raft_robust.Algorithm{Initiator: ids[0], Values: values},
									Delay:     delay, Seed: seed, LogOutput: devNull{},
									Byzantine: byz, MaxSteps: maxSteps,
								})
								steps := sim.Run()
								r = analyzeRaftRobust(sim, ids, values)
								r.steps = steps
								r.terminated = steps < maxSteps
							}
						} else {
							// f=0: nessun guasto, tutti e tre si comportano uguale
							switch algo {
							case "bully":
								sim := core.New(core.SimConfig{
									Nodes: ids, Topology: topo,
									Algorithm: &bully.Algorithm{Initiator: ids[0], Values: values},
									Delay:     delay, Seed: seed, LogOutput: devNull{},
									MaxSteps: maxSteps,
								})
								steps := sim.Run()
								r = analyzeBully(sim, ids, values)
								r.steps = steps
								r.terminated = steps < maxSteps
							case "raft":
								sim := core.New(core.SimConfig{
									Nodes: ids, Topology: topo,
									Algorithm: &raft.Algorithm{Initiator: ids[0], Values: values},
									Delay:     delay, Seed: seed, LogOutput: devNull{},
									MaxSteps: maxSteps,
								})
								steps := sim.Run()
								r = analyzeRaft(sim, ids, values)
								r.steps = steps
								r.terminated = steps < maxSteps
							case "raft_robust":
								sim := core.New(core.SimConfig{
									Nodes: ids, Topology: topo,
									Algorithm: &raft_robust.Algorithm{Initiator: ids[0], Values: values},
									Delay:     delay, Seed: seed, LogOutput: devNull{},
									MaxSteps: maxSteps,
								})
								steps := sim.Run()
								r = analyzeRaftRobust(sim, ids, values)
								r.steps = steps
								r.terminated = steps < maxSteps
							}
						}
					}

					r.algorithm = algo
					r.failureType = sc.name
					r.n = n
					r.f = f
					r.seed = seed

					le := strconv.FormatBool(r.leaderElected)
					lc := strconv.FormatBool(r.leaderCorrect)
					te := strconv.FormatBool(r.terminated)

					w.Write([]string{
						r.algorithm, r.failureType,
						strconv.Itoa(r.n), strconv.Itoa(r.f),
						strconv.FormatInt(r.seed, 10), strconv.Itoa(rep),
						le, lc,
						fmt.Sprintf("%.4f", r.leaderAgreement),
						fmt.Sprintf("%.4f", r.valuesCommitted),
						fmt.Sprintf("%.4f", r.valuesCorrect),
						strconv.Itoa(r.steps), te,
					})

					done++
					pct := float64(done) / float64(total) * 100
					t := "✓"
					if !r.terminated {
						t = "✗"
					}
					fmt.Printf("\r  [%5.1f%%] %-12s f=%-2d %-15s rep=%d  ldr=%v(%v) agr=%.2f val=%.2f %s",
						pct, algo, f, sc.name, rep,
						r.leaderElected, r.leaderCorrect,
						r.leaderAgreement, r.valuesCorrect, t)
				}
				fmt.Println()
			}
		}
		fmt.Println()
	}

	fmt.Printf("Results written to comparison_results.csv\n")
}

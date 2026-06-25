package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"strconv"

	"diasim/examples/bully"
	"diasim/examples/raft"
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
	leaderCorrect   bool    // elected leader is not Byzantine
	leaderAgreement float64 // fraction of correct nodes agreeing on same leader
	valuesCommitted float64 // fraction of correct nodes that committed all values
	valuesCorrect   float64 // fraction of correct nodes that committed the CORRECT values
	steps           int
	terminated      bool
}

// ── Analysis helpers ─────────────────────────────────────────────────────────

func analyzeRaft(sim *core.Simulator, ids []core.NodeID, values []any) result {
	var r result

	// Leader
	leader := raft.LeaderID(sim, ids)
	r.leaderElected = leader != ""
	if leader != "" {
		ln := sim.NodeState(leader)
		r.leaderCorrect = ln != nil && !ln.IsByzantine()
	}

	// Leader agreement: what fraction of correct nodes see the same leader?
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

	// Values committed
	committed := 0
	correct := 0
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

func analyzeBully(sim *core.Simulator, ids []core.NodeID, values []any) result {
	var r result

	// Leader
	leader := bully.LeaderID(sim, ids)
	r.leaderElected = leader != ""
	if leader != "" {
		ln := sim.NodeState(leader)
		r.leaderCorrect = ln != nil && !ln.IsByzantine()
	}

	// Leader agreement
	agreedLeader, count, total := bully.AgreedLeader(sim, ids)
	_ = agreedLeader
	if total > 0 {
		r.leaderAgreement = float64(count) / float64(total)
	}

	// Values committed (any order for Bully)
	committed := 0
	correct := 0
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		cv := bully.Committed(sim, id)
		if len(cv) >= len(values) {
			committed++
			// Check if all expected values are present (any order).
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
	correctCount := total
	if correctCount > 0 {
		r.valuesCommitted = float64(committed) / float64(correctCount)
		r.valuesCorrect = float64(correct) / float64(correctCount)
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

	outFile, err := os.Create("election_results.csv")
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

	fmt.Printf("DIASIM Election Benchmark — n=%d, f=0..%d, %d CPUs, %d reps\n",
		n, maxF, runtime.NumCPU(), reps)
	fmt.Printf("Values: %v, Delay: SeededDelay{1,10}, MaxSteps: %d\n\n", values, maxSteps)

	algos := []string{"bully", "raft"}

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

	for _, sc := range scenarios {
		fmt.Printf("═══ %s ═══\n%s\n\n", sc.name, sc.desc)

		for f := 0; f <= maxF; f++ {
			// Faulty nodes: last f nodes (N31-f..N30)
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
						}

					case "byz_active":
						var byz *core.ByzantineConfig
						if f > 0 {
							var forged any
							switch algo {
							case "bully":
								forged = bully.Msg{Kind: "REPLICATE", From: "FAKE", Values: []any{"FORGED"}}
							case "raft":
								forged = raft.Msg{Kind: "LOG_REQ", LeaderID: "FAKE", LTerm: 999}
							}
							byz = &core.ByzantineConfig{
								FixedByzantineNodes: faultyFollowers,
								Adversary:           core.ForgeAndFloodAdversary{ForgedValue: forged},
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
						}

					case "byz_leader":
						// The highest-priority node (N30) is Byzantine.
						// In Bully, N30 ALWAYS wins → Byzantine leader guaranteed.
						// In Raft, N30 might win (highest node index → latest election timeout)
						// but correct nodes can potentially elect someone else.
						if f >= 1 {
							byzNodes := []core.NodeID{ids[n-1]} // N30 (highest priority)
							if f > 1 {
								start := n - f
								if start < 1 {
									start = 1
								}
								for _, id := range ids[start : n-1] {
									byzNodes = append(byzNodes, id)
								}
							}
							var adversary core.ByzantineAdversary
							switch algo {
							case "bully":
								adversary = core.EquivocatingAdversary{
									CorruptedValue:  bully.Msg{Kind: "REPLICATE", From: ids[n-1], Values: []any{"FAKE1", "FAKE2", "FAKE3"}},
									CorruptFraction: 0.5,
								}
							case "raft":
								adversary = core.EquivocatingAdversary{
									CorruptedValue:  raft.Msg{Kind: "LOG_REQ", LeaderID: ids[n-1], LTerm: 1, Suffix: []raft.LogEntry{{Term: 1, Value: "FAKE"}}},
									CorruptFraction: 0.5,
								}
							}
							byz := &core.ByzantineConfig{
								FixedByzantineNodes: byzNodes,
								Adversary:           adversary,
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
							}
						} else {
							// f=0: no failures
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
							}
						}
					}

					r.algorithm = algo
					r.failureType = sc.name
					r.n = n
					r.f = f
					r.seed = seed

					le := "false"
					if r.leaderElected {
						le = "true"
					}
					lc := "false"
					if r.leaderCorrect {
						lc = "true"
					}
					te := "true"
					if !r.terminated {
						te = "false"
					}

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
					fmt.Printf("\r  [%5.1f%%] %-6s f=%-2d %-15s rep=%d  ldr=%v(%v) agr=%.2f val=%.2f %s",
						pct, algo, f, sc.name, rep,
						r.leaderElected, r.leaderCorrect,
						r.leaderAgreement, r.valuesCorrect, t)
				}
				fmt.Println()
			}
		}
		fmt.Println()
	}

	fmt.Printf("Results written to election_results.csv\n")
}

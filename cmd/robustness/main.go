package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"strconv"

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

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }

// ── Result ───────────────────────────────────────────────────────────────────

type result struct {
	algorithm    string
	failureType  string
	n, f         int
	seed         int64
	delivered    int // correct nodes that got the CORRECT value
	totalCorrect int
	agreed       int // nodes in the majority group
	anyDelivered int // correct nodes that got ANY value
	steps        int
	terminated   bool // false if simulation was cut off by MaxSteps
}

func (r result) deliveryRate() float64 {
	if r.totalCorrect == 0 {
		return 0
	}
	return float64(r.delivered) / float64(r.totalCorrect)
}

func (r result) agreementRate() float64 {
	if r.anyDelivered == 0 {
		return 1.0
	}
	return float64(r.agreed) / float64(r.anyDelivered)
}

// ── Generic analysis ─────────────────────────────────────────────────────────

func analyze(sim *core.Simulator, ids []core.NodeID, stateKey string, correctValue any) result {
	var r result
	values := make(map[string]int)
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		r.totalCorrect++
		v, ok := n.Get(stateKey)
		if ok {
			r.anyDelivered++
			key := fmt.Sprintf("%v", v)
			values[key]++
			if v == correctValue {
				r.delivered++
			}
		}
	}
	for _, c := range values {
		if c > r.agreed {
			r.agreed = c
		}
	}
	return r
}

// ── Main ─────────────────────────────────────────────────────────────────────

func main() {
	n := 31
	brachaF := (n - 1) / 3 // 10
	maxTestF := 2 * n / 3  // 20
	reps := 10
	maxStepsForACK := 5000 // cut off flooding_ack if it doesn't terminate

	seeds := make([]int64, reps)
	for i := range seeds {
		seeds[i] = int64(i*17 + 42)
	}

	ids := makeIDs(n)
	topo := topology.FullMesh(ids)
	correctValue := "correct_value"
	delay := &core.SeededDelay{Min: 1, Max: 10}

	outFile, err := os.Create("cmd/robustness/robustness_results.csv")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	w := csv.NewWriter(outFile)
	defer w.Flush()

	w.Write([]string{
		"algorithm", "failure_type", "n", "f", "seed", "repetition",
		"delivered_correct", "any_delivered", "total_correct",
		"delivery_rate", "agreed", "agreement_rate",
		"steps", "terminated",
	})

	fmt.Printf("DIASIM Robustness — n=%d, brachaF=%d, testF=0..%d, %d CPUs, %d reps\n",
		n, brachaF, maxTestF, runtime.NumCPU(), reps)
	fmt.Printf("Delay: SeededDelay{1, 10}, MaxSteps for ACK: %d\n\n", maxStepsForACK)

	allAlgos := []string{"flooding_naive", "flooding_ack", "bracha"}

	type scenario struct {
		name string
		desc string
	}

	scenarios := []scenario{
		{"crash_temporary", "f nodes crash at t=1, recover at t=15"},
		{"crash_permanent", "f nodes crash permanently at t=1"},
		{"byz_active", "f Byzantine followers forge-and-flood fake values"},
		{"byz_broadcaster", "Byzantine broadcaster equivocates + f-1 followers amplify"},
	}

	total := len(scenarios) * len(allAlgos) * (maxTestF + 1) * reps
	done := 0

	for _, sc := range scenarios {
		fmt.Printf("═══ %s ═══\n%s\n\n", sc.name, sc.desc)

		for f := 0; f <= maxTestF; f++ {
			// Determine faulty nodes (never include N0 for non-broadcaster scenarios).
			var faultyFollowers []core.NodeID
			if f > 0 {
				start := n - f
				if start < 1 {
					start = 1
				}
				faultyFollowers = ids[start:]
			}

			for _, algo := range allAlgos {
				for rep, seed := range seeds {
					var r result

					// Build failure config based on scenario and algorithm.
					var crashes map[core.NodeID]int64
					var byz *core.ByzantineConfig
					maxSteps := 0

					switch sc.name {

					case "crash_temporary":
						crashes = make(map[core.NodeID]int64)
						recoveries := make(map[core.NodeID]int64)
						for _, id := range faultyFollowers {
							crashes[id] = 1
							recoveries[id] = 15
						}
						// Build per-algo sim with recoveries.
						switch algo {
						case "flooding_naive":
							sim := core.New(core.SimConfig{
								Nodes: ids, Topology: topo,
								Algorithm: &flooding_naive.Algorithm{Initiator: ids[0], Value: correctValue},
								Delay:     delay, Seed: seed, LogOutput: devNull{},
								ScheduledCrashes: crashes, ScheduledRecoveries: recoveries,
							})
							steps := sim.Run()
							r = analyze(sim, ids, "naive_value", correctValue)
							r.steps = steps
							r.terminated = true
						case "flooding_ack":
							sim := core.New(core.SimConfig{
								Nodes: ids, Topology: topo,
								Algorithm: &flooding.Algorithm{Initiator: ids[0], Value: correctValue},
								Delay:     delay, Seed: seed, LogOutput: devNull{},
								ScheduledCrashes: crashes, ScheduledRecoveries: recoveries,
							})
							steps := sim.Run()
							r = analyze(sim, ids, "flood_value", correctValue)
							r.steps = steps
							r.terminated = true
						case "bracha":
							sim := core.New(core.SimConfig{
								Nodes: ids, Topology: topo,
								Algorithm: &bracha.Algorithm{Broadcaster: ids[0], Value: correctValue, F: brachaF},
								Delay:     delay, Seed: seed, LogOutput: devNull{},
								ScheduledCrashes: crashes, ScheduledRecoveries: recoveries,
							})
							steps := sim.Run()
							r = analyze(sim, ids, "bracha_value", correctValue)
							r.steps = steps
							r.terminated = true
						}
						goto writeResult

					case "crash_permanent":
						crashes = make(map[core.NodeID]int64)
						for _, id := range faultyFollowers {
							crashes[id] = 1
						}
						if algo == "flooding_ack" {
							maxSteps = maxStepsForACK // prevent hanging
						}

					case "byz_active":
						if f > 0 {
							var forgedPayload any
							switch algo {
							case "flooding_naive":
								forgedPayload = flooding_naive.Msg{Value: "FORGED"}
							case "flooding_ack":
								forgedPayload = flooding.Msg{Kind: "FLOOD", Value: "FORGED"}
							case "bracha":
								forgedPayload = bracha.Msg{Kind: "ECHO", Value: "FORGED"}
							}
							byz = &core.ByzantineConfig{
								FixedByzantineNodes: faultyFollowers,
								Adversary:           core.ForgeAndFloodAdversary{ForgedValue: forgedPayload},
							}
						}
						if algo == "flooding_ack" {
							maxSteps = maxStepsForACK
						}

					case "byz_broadcaster":
						if f >= 1 {
							byzNodes := []core.NodeID{ids[0]} // broadcaster
							if f > 1 {
								start := n - (f - 1)
								if start < 1 {
									start = 1
								}
								byzNodes = append(byzNodes, ids[start:]...)
							}
							var corruptPayload any
							switch algo {
							case "flooding_naive":
								corruptPayload = flooding_naive.Msg{Value: "EQUIVOCATED"}
							case "flooding_ack":
								corruptPayload = flooding.Msg{Kind: "FLOOD", Value: "EQUIVOCATED"}
							case "bracha":
								corruptPayload = bracha.Msg{Kind: "SEND", Value: "EQUIVOCATED"}
							}
							byz = &core.ByzantineConfig{
								FixedByzantineNodes: byzNodes,
								Adversary: core.EquivocatingAdversary{
									CorruptedValue:  corruptPayload,
									CorruptFraction: 0.5,
								},
							}
						}
						if algo == "flooding_ack" {
							maxSteps = maxStepsForACK
						}
					}

					// Run simulation.
					switch algo {
					case "flooding_naive":
						sim := core.New(core.SimConfig{
							Nodes: ids, Topology: topo,
							Algorithm: &flooding_naive.Algorithm{Initiator: ids[0], Value: correctValue},
							Delay:     delay, Seed: seed, LogOutput: devNull{},
							ScheduledCrashes: crashes, Byzantine: byz, MaxSteps: maxSteps,
						})
						steps := sim.Run()
						r = analyze(sim, ids, "naive_value", correctValue)
						r.steps = steps
						r.terminated = (maxSteps == 0 || steps < maxSteps)

					case "flooding_ack":
						sim := core.New(core.SimConfig{
							Nodes: ids, Topology: topo,
							Algorithm: &flooding.Algorithm{Initiator: ids[0], Value: correctValue},
							Delay:     delay, Seed: seed, LogOutput: devNull{},
							ScheduledCrashes: crashes, Byzantine: byz, MaxSteps: maxSteps,
						})
						steps := sim.Run()
						r = analyze(sim, ids, "flood_value", correctValue)
						r.steps = steps
						r.terminated = (maxSteps == 0 || steps < maxSteps)

					case "bracha":
						sim := core.New(core.SimConfig{
							Nodes: ids, Topology: topo,
							Algorithm: &bracha.Algorithm{Broadcaster: ids[0], Value: correctValue, F: brachaF},
							Delay:     delay, Seed: seed, LogOutput: devNull{},
							ScheduledCrashes: crashes, Byzantine: byz, MaxSteps: maxSteps,
						})
						steps := sim.Run()
						r = analyze(sim, ids, "bracha_value", correctValue)
						r.steps = steps
						r.terminated = (maxSteps == 0 || steps < maxSteps)
					}

				writeResult:
					r.algorithm = algo
					r.failureType = sc.name
					r.n = n
					r.f = f
					r.seed = seed

					termStr := "true"
					if !r.terminated {
						termStr = "false"
					}

					w.Write([]string{
						r.algorithm, r.failureType,
						strconv.Itoa(r.n), strconv.Itoa(r.f),
						strconv.FormatInt(r.seed, 10), strconv.Itoa(rep),
						strconv.Itoa(r.delivered), strconv.Itoa(r.anyDelivered),
						strconv.Itoa(r.totalCorrect),
						fmt.Sprintf("%.4f", r.deliveryRate()),
						strconv.Itoa(r.agreed),
						fmt.Sprintf("%.4f", r.agreementRate()),
						strconv.Itoa(r.steps), termStr,
					})

					done++
					pct := float64(done) / float64(total) * 100
					t := "✓"
					if !r.terminated {
						t = "✗"
					}
					fmt.Printf("\r  [%5.1f%%] %-15s f=%-2d %-18s rep=%d  del=%.2f agr=%.2f %s (%d/%d/%d)",
						pct, algo, f, sc.name, rep,
						r.deliveryRate(), r.agreementRate(), t,
						r.delivered, r.anyDelivered, r.totalCorrect)
				}
				fmt.Println()
			}
		}
		fmt.Println()
	}

	fmt.Printf("Results written to robustness_results.csv\n")
}

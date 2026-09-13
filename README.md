# DIASIM: A Deterministic Simulator for Distributed Algorithms

DIASIM is a deterministic, discrete-event simulator for distributed algorithms,
written in Go as a modular library. Distributed algorithms are expressed as
collections of local behaviours reacting to messages and logical-time events;
the simulator orchestrates execution, decides which events occur and when,
injects failures, and records detailed execution traces. This separation of
concerns keeps algorithm code simple and declarative while execution semantics
stay centralized, well-defined, and fully reproducible.

Given the same configuration and the same random seed, a run always produces the
same result. Determinism is the property everything else rests on: it is what
makes debugging tractable and what turns one-off measurements into reproducible
experiments.

> This repository accompanies a master's thesis. The `cmd/` benchmarks and the
> CSV files they produce are the data behind the thesis's evaluation chapters.

---

## Table of contents

- [Key ideas](#key-ideas)
- [Repository layout](#repository-layout)
- [Requirements & quick start](#requirements--quick-start)
- [Writing an algorithm](#writing-an-algorithm)
- [Configuring a simulation](#configuring-a-simulation)
- [Execution modes: sequential and parallel](#execution-modes-sequential-and-parallel)
- [Failure injection](#failure-injection)
- [Algorithms included](#algorithms-included)
- [Benchmarks and reproducing the data](#benchmarks-and-reproducing-the-data)
- [Tests](#tests)
- [Design notes](#design-notes)

---

## Key ideas

- **Discrete-event core.** A single logical clock and a global event queue. Time
  advances only when an event is processed; there is no wall-clock time in the
  model. Events are totally ordered by `(Time, SeqNum)`, which makes the ordering
  deterministic.
- **Compute–commit execution.** An algorithm handler never acts directly: calls
  to `Send` and `SetTimer` only record *intentions* in a private buffer. The
  simulator's commit phase decides, centrally, whether and when each intention
  becomes a real effect. This is what keeps the algorithm interface identical
  across execution modes and what makes failure injection external to the
  algorithms.
- **Separation of algorithm and execution.** An algorithm describes *what* a node
  does when something happens to it; it says nothing about *when* things happen or
  what happens on failure. All of that is the simulator's responsibility.
- **Determinism.** Same configuration + same seed ⇒ same run. The parallel engine
  preserves the *algorithmic outcome* exactly (see below).

## Repository layout

```
pkg/core/          the simulator core
  types.go         Event, Message, Action, EventType
  algorithm.go     Algorithm and DelayModel interfaces; Fixed/PerLink/Seeded delays
  node.go          Node and its narrow API (identity, status, state, Send, SetTimer)
  eventqueue.go    binary-heap event queue
  simulator.go     SimConfig, New(), sequential run loop, compute/commit
  parallel.go      CMB-based parallel engine (barrier synchronization)
  lp.go            Logical Process, unbounded inbox, barrier loop
  byzatine.go      ByzantineAdversary interface + four ready-made adversaries
  logger.go        structured, thread-safe logging

pkg/topology/      topology graph: Ring, FullMesh, or custom (AddEdge/AddBiEdge)

examples/          algorithms implemented against the core
  flooding_naive/  flooding, no fault tolerance (baseline)
  flooding/        flooding with ACK + retransmission
  bully/           Bully leader election + majority-commit replication
  raft/            Raft (event-driven formulation)
  raft_robust/     Raft with two Byzantine-hardening modifications

cmd/               runnable programs
  example/         guided demos of flooding under various failures
  benchmark/       scalability benchmark  -> benchmark_results.csv
  robustness/      broadcast robustness   -> robustness_results.csv
  comparison/      election comparison incl. raft_robust -> comparison_results.csv
  election_robustness/ Bully vs Raft election -> election_results.csv

thesis_figures.py  regenerates the thesis figure coordinates from the CSVs
```

## Requirements & quick start

- **Go 1.22** or newer. No external dependencies (standard library only).

```bash
# clone
git clone https://github.com/IreneScarpanti/DIASIM.git
cd DIASIM

# run the guided demos (flooding, with and without failures)
go run ./cmd/example

# run the test suite
go test ./...
```

A minimal simulation from Go:

```go
ids := []core.NodeID{"A", "B", "C"}
sim := core.New(core.SimConfig{
    Nodes:     ids,
    Topology:  topology.FullMesh(ids),
    Algorithm: &flooding.Algorithm{Initiator: "A", Value: "hello-world"},
    Delay:     &core.FixedDelay{Min: 1, Max: 1},
    Seed:      42,
    LogLevel:  core.LevelInfo,
})
sim.Run()
```

## Writing an algorithm

An algorithm implements a three-method interface — the entire contract between an
algorithm and the engine:

```go
type Algorithm interface {
    OnStart(n *Node)              // called once when a node is initialised
    OnMessage(n *Node, msg Message) // called when a message is delivered
    OnTick(n *Node)              // called when a timer fires
}
```

Inside a handler, a node can only do what a real node in an asynchronous
message-passing system could: inspect its own identity and status, query the
topology for its neighbours, read and write its own key-value state, and record
two kinds of intention.

```go
n.ID()                     // this node's identifier
n.Status()                 // alive / crashed
n.Neighbors()              // neighbours in the (immutable) topology
n.IsNeighbor(to)           // is there an edge to `to`?
n.Get(key) / n.Set(key,v)  // private per-node key-value store

n.Send(to, payload)        // record intent to send (applied at commit)
n.SetTimer(delay)          // record intent to set a timer (delay >= 1)
```

A node has no way to inspect the event queue, wall-clock time, or another node's
state — this is the system model enforced at the level of the API.

## Configuring a simulation

Everything is set through `SimConfig`:

| Field | Meaning |
|---|---|
| `Nodes` | list of node IDs |
| `Topology` | any `TopologyReader` (`topology.Ring`, `topology.FullMesh`, or a custom `Graph`) |
| `Algorithm` | the algorithm every node runs |
| `Delay` | message-delay model (see below); defaults to `FixedDelay{1,1}` |
| `Seed` | the seed that makes the run reproducible |
| `Failures` | probabilistic crash/link-failure schedule (optional) |
| `Byzantine` | Byzantine node set + adversary (optional) |
| `ScheduledCrashes` / `ScheduledRecoveries` | deterministic per-node crash/recovery times |
| `Mode` | `ModeSequential` (default) or `ModeParallel` |
| `BatchSize` | events computed together per timestamp (sequential engine) |
| `MaxSteps` | hard cap on processed events (guards against non-terminating runs) |
| `LogLevel` | `LevelDebug`, `LevelInfo`, `LevelWarn` |
| `LogOutput` | where the streamed trace goes (`os.Stdout`, a file, `io.Discard`, …) |

**Delay models** (all draw from the single seeded RNG, so all are reproducible):

- `FixedDelay{Min, Max}` — one value, resolved once, applied to every message.
- `PerLinkDelay{Min, Max}` — one value per directed edge, resolved once.
- `SeededDelay{Min, Max}` — a fresh draw for every message.

`Run()` drives the whole simulation to completion and returns the number of
steps processed. The full trace is retrievable in memory via `sim.Logger().Entries()`,
which is how the verification helpers inspect a run without parsing text.

## Execution modes: sequential and parallel

The **sequential** engine is the default: a single thread, one event at a time,
in compute–commit order. It is the right choice for correctness work and for
algorithms whose per-event work is light.

The **parallel** engine (`Mode: core.ModeParallel`) runs each node as a *Logical
Process* on its own goroutine, using conservative, barrier-synchronized parallel
discrete-event simulation (the Chandy–Misra–Bryant discipline). It preserves the
sequential engine's **algorithmic outcome** exactly — every node ends in the same
state, every message is delivered or dropped for the same reason — while
distributing per-event work across cores. The one thing it does *not* fix is the
exact interleaving of log lines from different processes; the committed result is
identical, the order two unrelated log entries happen to be written may differ.

Parallelism pays only when per-event work is heavy enough to amortize the barrier
cost (e.g. Raft), and saturates well below the core count; for light workloads
(e.g. flooding) the sequential engine is faster. See `cmd/benchmark`.

## Failure injection

Failures are injected entirely at commit time, outside the algorithms — the same
algorithm runs unchanged with or without them. Three classes:

- **Crash** (permanent or temporary). A crashed node produces and receives
  nothing. Recovery is modelled as a *clean restart*: state is wiped and `OnStart`
  runs again, so a recovered node re-learns everything through the protocol.
- **Link failure** (directional). Messages on a failed `(from, to)` edge are
  dropped for the duration; the underlying topology is never modified.
- **Byzantine.** A Byzantine node runs the ordinary, correct algorithm; its
  misbehaviour is imposed from outside on its outgoing messages, through a
  `ByzantineAdversary`. Four are provided:
    - `SilentAdversary` — drops every outgoing message.
    - `CorruptingAdversary` — replaces every payload with a fixed wrong value.
    - `EquivocatingAdversary` — corrupts a configurable fraction of sends (tells
      different nodes different things).
    - `ForgeAndFloodAdversary` — sends a forged message to all neighbours.

Failures can be scheduled deterministically (`ScheduledCrashes` /
`ScheduledRecoveries`, `Byzantine.FixedByzantineNodes`) or probabilistically
(`FailureConfig`, `Byzantine.ByzantineNodeRate`); the seeded RNG keeps even the
probabilistic schedules reproducible.

## Algorithms included

| Package | Algorithm | Tolerates |
|---|---|---|
| `examples/flooding_naive` | Flooding, forward-once, no ACK | nothing (baseline) |
| `examples/flooding` | Flooding with ACK + retransmission | message loss, transient link/crash faults |
| `examples/bully` | Bully leader election + majority-commit replication | crash faults (no proof across leader changes) |
| `examples/raft` | Raft (event-driven `LogRequest`/`LogResponse` formulation) | crash faults, with a safety proof |
| `examples/raft_robust` | Raft + term bound + leader lease | as Raft, plus one class of Byzantine follower |

`raft_robust` carries two independent, thesis-specific modifications: a **term
bound** (reject messages whose term exceeds `n×5`, which neutralizes a forged-term
attack at no cost in fault-free runs) and a **leader lease** (documented, with its
measured limitation, in the thesis).

## Benchmarks and reproducing the data

Each command runs a fixed experiment matrix and writes a CSV. All use `n`, seeds,
and failure schedules fixed in the source, so results are reproducible.

```bash
go run ./cmd/benchmark      # scalability: sequential vs parallel, flooding & Raft
go run ./cmd/robustness     # broadcast robustness: naive/ACK flooding, Bracha
go run ./cmd/comparison     # election: Bully vs Raft vs Raft_Robust under faults
go run ./cmd/election_robustness  # election: Bully vs Raft baseline
```

Outputs: `benchmark_results.csv`, `robustness_results.csv`,
`comparison_results.csv`, `election_results.csv`.

### Reproducing the thesis figures

The evaluation figures in the thesis are TikZ/pgfplots sources whose coordinates
come directly from these CSVs. `thesis_figures.py` regenerates those exact
coordinates, so every figure is reproducible from the repository:

```bash
python3 thesis_figures.py            # reads the CSVs and prints the coordinates
```

For the election experiments a run is scored as a success when it **terminates
within its step budget and every correct node commits exactly the correct
values** (`terminated` AND `values_correct == 1`); the headline metric is the
number of successful runs out of ten seeds at each `f`. Broadcast panels report
the mean delivery and agreement rates over the ten seeds. Scalability figures
report the median wall time over the repetitions and the speedup
`median(sequential) / median(parallel)`.

## Tests

```bash
go test ./...
```

- `examples/flooding/flooding_test.go` — flooding behaviour.
- `pkg/core_test/parallel_test.go` — sequential/parallel equivalence.
- `pkg/core_test/byzantine_test.go` — Byzantine injection.

## Design notes

- **Event ordering** is `(Time, SeqNum)`: timestamp first, then a per-simulator
  monotone sequence number assigned at creation. The sequence number alone makes
  the order total and deterministic.
- **The inbox** connecting two Logical Processes is a mutex-protected, *unbounded*
  queue, so a sender never blocks — important for high-fan-out algorithms like a
  Raft leader messaging every follower within one barrier cycle.
- **Timers** are logical-time only; a delay below one is clamped to one, so time
  always strictly advances. There is no timer cancellation — algorithms simply
  ignore ticks that are no longer relevant.
- **The logger** is safe to call from multiple goroutines: every Logical Process
  writes to the same shared logger under the parallel engine.
```
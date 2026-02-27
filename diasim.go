// Package diasim is the entry point for the DIASIM distributed algorithm simulator.
//
// DIASIM is a deterministic, event-driven simulator designed for studying and
// teaching distributed algorithms. It provides:
//
//   - A compute–commit execution model ensuring full determinism given a fixed seed.
//   - A total ordering of events: (time, event_type, node_id, seq_num).
//   - Support for crash-stop node failures and link failures as first-class events.
//   - Structured logging of every simulation event.
//   - A clean Algorithm interface that keeps algorithm code free of simulator concerns.
//
// Quick start:
//
//	import (
//	    "github.com/diasim/pkg/core"
//	    "github.com/diasim/pkg/topology"
//	    "github.com/diasim/pkg/logging"
//	)
//
//	ids := []core.NodeID{"N0", "N1", "N2"}
//	topo := topology.Ring(ids)
//	algo := &MyAlgorithm{}
//	log  := logging.NewStdout()
//
//	sim := core.New(core.SimConfig{
//	    Nodes:     ids,
//	    Topology:  topo,
//	    Algorithm: algo,
//	    Delay:     core.FixedDelay{D: 1},
//	    Seed:      42,
//	    Logger:    log,
//	})
//	sim.Run()
package core

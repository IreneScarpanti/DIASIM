package topology

import "diasim/pkg/core"

type Graph struct {
	edges   map[core.NodeID][]core.NodeID
	edgeSet map[[2]core.NodeID]struct{}
}

func New() *Graph {
	return &Graph{
		edges:   make(map[core.NodeID][]core.NodeID),
		edgeSet: make(map[[2]core.NodeID]struct{}),
	}
}

func (g *Graph) AddNode(id core.NodeID) {
	if _, ok := g.edges[id]; !ok {
		g.edges[id] = nil
	}
}

func (g *Graph) AddEdge(from, to core.NodeID) {
	g.AddNode(from)
	g.AddNode(to)
	key := [2]core.NodeID{from, to}
	if _, exists := g.edgeSet[key]; !exists {
		g.edges[from] = append(g.edges[from], to)
		g.edgeSet[key] = struct{}{}
	}
}

func (g *Graph) AddBiEdge(a, b core.NodeID) {
	g.AddEdge(a, b)
	g.AddEdge(b, a)
}

func (g *Graph) Nodes() []core.NodeID {
	ids := make([]core.NodeID, 0, len(g.edges))
	for id := range g.edges {
		ids = append(ids, id)
	}
	return ids
}

func (g *Graph) Neighbors(id core.NodeID) []core.NodeID {
	return g.edges[id]
}

func (g *Graph) HasEdge(from, to core.NodeID) bool {
	_, ok := g.edgeSet[[2]core.NodeID{from, to}]
	return ok
}

func Ring(ids []core.NodeID) *Graph {
	g := New()
	k := len(ids)
	for i := 0; i < k; i++ {
		g.AddBiEdge(ids[i], ids[(i+1)%k])
	}
	return g
}

func FullMesh(ids []core.NodeID) *Graph {
	g := New()
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			g.AddBiEdge(ids[i], ids[j])
		}
	}
	return g
}

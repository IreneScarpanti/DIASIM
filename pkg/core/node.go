package core

type NodeStatus string

const (
	StatusAlive   NodeStatus = "ALIVE"
	StatusCrashed NodeStatus = "CRASHED"
)

type Node struct {
	id       NodeID
	status   NodeStatus
	state    map[string]any
	topology TopologyReader
	runtime  ActionBuffer
}

func NewNode(id NodeID, topo TopologyReader, buf ActionBuffer) *Node {
	return &Node{
		id:       id,
		status:   StatusAlive,
		state:    make(map[string]any),
		topology: topo,
		runtime:  buf,
	}
}

func (n *Node) ID() NodeID { return n.id }

func (n *Node) Status() NodeStatus { return n.status }

func (n *Node) SetStatus(s NodeStatus) { n.status = s }

func (n *Node) Neighbors() []NodeID { return n.topology.Neighbors(n.id) }

func (n *Node) IsNeighbor(to NodeID) bool { return n.topology.HasEdge(n.id, to) }

func (n *Node) Get(key string) (any, bool) {
	v, ok := n.state[key]
	return v, ok
}

func (n *Node) Set(key string, value any) { n.state[key] = value }

func (n *Node) ResetState() { n.state = make(map[string]any) }

func (n *Node) Send(to NodeID, payload any) {
	n.runtime.AddAction(Action{
		Type:    ActionSend,
		SendTo:  to,
		Payload: payload,
	})
}

func (n *Node) SetTimer(delay int64) {
	if delay < 1 {
		delay = 1
	}
	n.runtime.AddAction(Action{
		Type:  ActionSetTimer,
		Delay: delay,
	})
}

type TopologyReader interface {
	Neighbors(id NodeID) []NodeID
	HasEdge(from, to NodeID) bool
}

type ActionBuffer interface {
	AddAction(a Action)
}

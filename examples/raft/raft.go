// Package raft implements a simplified version of the Raft consensus algorithm
// for the DIASIM simulator.
//
// This implementation covers leader election and log replication (the two core
// subproblems of Raft) without membership changes or log compaction. It follows
// the Raft Lite pseudocode (Liangrun Da / Martin Kleppmann) adapted to DIASIM's
// Algorithm interface (OnStart / OnMessage / OnTick).
//
// Timer management: DIASIM provides a single OnTick callback. We use two timer
// durations to distinguish election timeouts from replication heartbeats:
//   - Election timeout:    electionBaseInterval + (node index * 2) to stagger
//   - Replication timeout: replicationInterval (shorter, leader-only)
//
// Client requests: a configurable set of client values are injected by the
// designated initiator node at OnStart time. The leader appends them to its log
// and replicates to followers. Values are "committed" (delivered) when a majority
// of nodes acknowledge them.
package raft

import (
	"fmt"

	"diasim/pkg/core"
)

// ── Timer intervals (in logical time units) ──────────────────────────────────

const (
	electionBaseInterval = 15 // base election timeout (staggered per node)
	electionJitter       = 2  // multiplier for node index staggering
	replicationInterval  = 5  // heartbeat / log replication interval
)

// ── Roles ────────────────────────────────────────────────────────────────────

type role string

const (
	roleFollower  role = "follower"
	roleCandidate role = "candidate"
	roleLeader    role = "leader"
)

// ── Message types ────────────────────────────────────────────────────────────

type msgKind string

const (
	kindVoteRequest  msgKind = "VOTE_REQ"
	kindVoteResponse msgKind = "VOTE_RESP"
	kindLogRequest   msgKind = "LOG_REQ"
	kindLogResponse  msgKind = "LOG_RESP"
	kindForward      msgKind = "FORWARD" // forward client request to leader
)

// Msg is the payload carried in all inter-node messages.
type Msg struct {
	Kind msgKind

	// VoteRequest fields
	CandidateID core.NodeID
	CTerm       int64
	CLogLength  int
	CLogTerm    int64

	// VoteResponse fields
	VoterID core.NodeID
	VTerm   int64
	Granted bool

	// LogRequest fields
	LeaderID     core.NodeID
	LTerm        int64
	PrefixLen    int
	PrefixTerm   int64
	LeaderCommit int
	Suffix       []LogEntry

	// LogResponse fields
	FollowerID core.NodeID
	FTerm      int64
	Ack        int
	Success    bool

	// Forward fields
	ForwardValue any
}

func (m Msg) String() string {
	switch m.Kind {
	case kindVoteRequest:
		return fmt.Sprintf("VoteReq(cand=%s,term=%d)", m.CandidateID, m.CTerm)
	case kindVoteResponse:
		return fmt.Sprintf("VoteResp(voter=%s,term=%d,ok=%v)", m.VoterID, m.VTerm, m.Granted)
	case kindLogRequest:
		return fmt.Sprintf("LogReq(leader=%s,term=%d,pfx=%d,sfx=%d)", m.LeaderID, m.LTerm, m.PrefixLen, len(m.Suffix))
	case kindLogResponse:
		return fmt.Sprintf("LogResp(follower=%s,term=%d,ack=%d,ok=%v)", m.FollowerID, m.FTerm, m.Ack, m.Success)
	case kindForward:
		return fmt.Sprintf("Forward(val=%v)", m.ForwardValue)
	default:
		return fmt.Sprintf("RaftMsg(%s)", m.Kind)
	}
}

// LogEntry represents a single entry in the replicated log.
type LogEntry struct {
	Term  int64
	Value any
}

// ── State keys ───────────────────────────────────────────────────────────────

const (
	sCurrentTerm    = "raft_currentTerm"
	sVotedFor       = "raft_votedFor"
	sCurrentRole    = "raft_currentRole"
	sCurrentLeader  = "raft_currentLeader"
	sVotesReceived  = "raft_votesReceived"
	sLog            = "raft_log"
	sCommitLength   = "raft_commitLength"
	sSentLength     = "raft_sentLength"
	sAckedLength    = "raft_ackedLength"
	sNodeIndex      = "raft_nodeIndex"
	sDelivered      = "raft_delivered" // []any — committed values delivered
	sAllNodes       = "raft_allNodes"
	sValuesInjected = "raft_valuesInjected"
	sLastHeartbeat  = "raft_lastHeartbeat" // logical time of last heartbeat/vote grant
	sTickCount      = "raft_tickCount"     // number of ticks fired so far
)

// ── Algorithm ────────────────────────────────────────────────────────────────

// Algorithm implements the DIASIM Algorithm interface for simplified Raft.
type Algorithm struct {
	// Values is the list of client values to be proposed. The Initiator node
	// injects these into the Raft log via the leader.
	Values []any

	// Initiator is the node that proposes client values at startup.
	Initiator core.NodeID
}

func (a *Algorithm) OnStart(n *core.Node) {
	neighbors := n.Neighbors()
	allNodes := append([]core.NodeID{n.ID()}, neighbors...)

	// Compute a stable node index for election timeout staggering.
	nodeIndex := 0
	for _, nb := range neighbors {
		if nb < n.ID() {
			nodeIndex++
		}
	}

	n.Set(sCurrentTerm, int64(0))
	n.Set(sVotedFor, core.NodeID(""))
	n.Set(sCurrentRole, roleFollower)
	n.Set(sCurrentLeader, core.NodeID(""))
	n.Set(sVotesReceived, map[core.NodeID]bool{})
	n.Set(sLog, []LogEntry{})
	n.Set(sCommitLength, 0)
	n.Set(sSentLength, make(map[core.NodeID]int))
	n.Set(sAckedLength, make(map[core.NodeID]int))
	n.Set(sNodeIndex, nodeIndex)
	n.Set(sDelivered, []any{})
	n.Set(sAllNodes, allNodes)
	n.Set(sValuesInjected, false)
	n.Set(sLastHeartbeat, int64(0))
	n.Set(sTickCount, int64(0))

	// Single recurring tick. The tick interval is the replication interval.
	// Election timeout is checked by comparing tick count against a per-node
	// threshold derived from nodeIndex.
	n.SetTimer(replicationInterval)
}

func (a *Algorithm) OnMessage(n *core.Node, msg core.Message) {
	payload, ok := msg.Payload.(Msg)
	if !ok {
		return
	}

	switch payload.Kind {
	case kindVoteRequest:
		a.handleVoteRequest(n, payload)
	case kindVoteResponse:
		a.handleVoteResponse(n, payload)
	case kindLogRequest:
		a.handleLogRequest(n, payload)
	case kindLogResponse:
		a.handleLogResponse(n, payload)
	case kindForward:
		a.handleForward(n, payload)
	}
}

func (a *Algorithm) OnTick(n *core.Node) {
	if a.isDone(n) {
		return // stop ticking when all values committed
	}

	tickCount := getTickCount(n) + 1
	n.Set(sTickCount, tickCount)

	currentRole := getRole(n)

	if currentRole == roleLeader {
		// Leader: replicate log / send heartbeats.
		for _, nb := range n.Neighbors() {
			a.replicateLog(n, nb)
		}
	} else {
		// Follower or candidate: check if election timeout has elapsed.
		// Each node has a different threshold based on its index.
		nodeIndex, _ := n.Get(sNodeIndex)
		idx, _ := nodeIndex.(int)
		electionTicks := int64(electionBaseInterval/replicationInterval) + int64(idx)
		lastHB := getLastHeartbeat(n)

		if tickCount-lastHB >= electionTicks {
			a.handleElectionTimeout(n)
		}
	}

	// Schedule next tick.
	n.SetTimer(replicationInterval)
}

// ── Election timeout ─────────────────────────────────────────────────────────

func (a *Algorithm) handleElectionTimeout(n *core.Node) {
	currentRole := getRole(n)
	if currentRole == roleLeader {
		// Leaders don't start elections; reset election timer as safety.
		a.resetElectionTimer(n)
		return
	}

	currentTerm := getTerm(n)
	currentTerm++
	n.Set(sCurrentTerm, currentTerm)
	n.Set(sCurrentRole, roleCandidate)
	n.Set(sVotedFor, n.ID())
	n.Set(sVotesReceived, map[core.NodeID]bool{n.ID(): true})

	log := getLog(n)
	lastTerm := int64(0)
	if len(log) > 0 {
		lastTerm = log[len(log)-1].Term
	}

	for _, nb := range n.Neighbors() {
		n.Send(nb, Msg{
			Kind:        kindVoteRequest,
			CandidateID: n.ID(),
			CTerm:       currentTerm,
			CLogLength:  len(log),
			CLogTerm:    lastTerm,
		})
	}

	a.resetElectionTimer(n)
}

// ── VoteRequest ──────────────────────────────────────────────────────────────

func (a *Algorithm) handleVoteRequest(n *core.Node, msg Msg) {
	currentTerm := getTerm(n)

	if msg.CTerm > currentTerm {
		currentTerm = msg.CTerm
		n.Set(sCurrentTerm, currentTerm)
		n.Set(sCurrentRole, roleFollower)
		n.Set(sVotedFor, core.NodeID(""))
		a.resetElectionTimer(n)
	}

	log := getLog(n)
	lastTerm := int64(0)
	if len(log) > 0 {
		lastTerm = log[len(log)-1].Term
	}

	logOk := msg.CLogTerm > lastTerm ||
		(msg.CLogTerm == lastTerm && msg.CLogLength >= len(log))

	votedFor := getVotedFor(n)
	granted := false
	if msg.CTerm == currentTerm && logOk && (votedFor == "" || votedFor == msg.CandidateID) {
		n.Set(sVotedFor, msg.CandidateID)
		granted = true
		a.resetElectionTimer(n)
	}

	n.Send(msg.CandidateID, Msg{
		Kind:    kindVoteResponse,
		VoterID: n.ID(),
		VTerm:   currentTerm,
		Granted: granted,
	})
}

// ── VoteResponse ─────────────────────────────────────────────────────────────

func (a *Algorithm) handleVoteResponse(n *core.Node, msg Msg) {
	currentTerm := getTerm(n)
	currentRole := getRole(n)

	if currentRole == roleCandidate && msg.VTerm == currentTerm && msg.Granted {
		votes := getVotes(n)
		votes[msg.VoterID] = true
		n.Set(sVotesReceived, votes)

		allNodes := getAllNodes(n)
		majority := (len(allNodes) + 1) / 2
		if len(votes) >= majority {
			// Won election — become leader.
			n.Set(sCurrentRole, roleLeader)
			n.Set(sCurrentLeader, n.ID())

			log := getLog(n)
			sentLength := make(map[core.NodeID]int)
			ackedLength := make(map[core.NodeID]int)
			for _, nb := range n.Neighbors() {
				sentLength[nb] = len(log)
				ackedLength[nb] = 0
			}
			ackedLength[n.ID()] = len(log)
			n.Set(sSentLength, sentLength)
			n.Set(sAckedLength, ackedLength)

			// Inject client values now that we are leader.
			a.injectValues(n)

			// Send initial heartbeat / log replication to all followers.
			for _, nb := range n.Neighbors() {
				a.replicateLog(n, nb)
			}
		}
	} else if msg.VTerm > currentTerm {
		n.Set(sCurrentTerm, msg.VTerm)
		n.Set(sCurrentRole, roleFollower)
		n.Set(sVotedFor, core.NodeID(""))
		a.resetElectionTimer(n)
	}
}

// ── Broadcast / Forward ──────────────────────────────────────────────────────

func (a *Algorithm) handleForward(n *core.Node, msg Msg) {
	if getRole(n) == roleLeader {
		a.appendAndReplicate(n, msg.ForwardValue)
	}
	// If not leader, drop — the sender will retry via election.
}

func (a *Algorithm) appendAndReplicate(n *core.Node, value any) {
	currentTerm := getTerm(n)
	log := getLog(n)
	log = append(log, LogEntry{Term: currentTerm, Value: value})
	n.Set(sLog, log)

	ackedLength := getAckedLength(n)
	ackedLength[n.ID()] = len(log)
	n.Set(sAckedLength, ackedLength)

	for _, nb := range n.Neighbors() {
		a.replicateLog(n, nb)
	}
}

func (a *Algorithm) injectValues(n *core.Node) {
	if n.ID() != a.Initiator {
		return
	}
	injected, _ := n.Get(sValuesInjected)
	if injected == true {
		return
	}
	n.Set(sValuesInjected, true)

	for _, v := range a.Values {
		a.appendAndReplicate(n, v)
	}
}

// ── ReplicateLog ─────────────────────────────────────────────────────────────

func (a *Algorithm) replicateLog(n *core.Node, followerID core.NodeID) {
	log := getLog(n)
	sentLength := getSentLength(n)
	prefixLen := sentLength[followerID]
	if prefixLen > len(log) {
		prefixLen = len(log)
	}

	suffix := make([]LogEntry, len(log)-prefixLen)
	copy(suffix, log[prefixLen:])

	prefixTerm := int64(0)
	if prefixLen > 0 {
		prefixTerm = log[prefixLen-1].Term
	}

	n.Send(followerID, Msg{
		Kind:         kindLogRequest,
		LeaderID:     n.ID(),
		LTerm:        getTerm(n),
		PrefixLen:    prefixLen,
		PrefixTerm:   prefixTerm,
		LeaderCommit: getCommitLength(n),
		Suffix:       suffix,
	})
}

// ── LogRequest ───────────────────────────────────────────────────────────────

func (a *Algorithm) handleLogRequest(n *core.Node, msg Msg) {
	currentTerm := getTerm(n)

	if msg.LTerm > currentTerm {
		currentTerm = msg.LTerm
		n.Set(sCurrentTerm, currentTerm)
		n.Set(sVotedFor, core.NodeID(""))
		a.resetElectionTimer(n)
	}

	if msg.LTerm == currentTerm {
		n.Set(sCurrentRole, roleFollower)
		n.Set(sCurrentLeader, msg.LeaderID)
		a.resetElectionTimer(n)

		// If we just discovered the leader and we are the initiator without
		// injected values, forward them.
		if n.ID() == a.Initiator {
			injected, _ := n.Get(sValuesInjected)
			if injected != true {
				n.Set(sValuesInjected, true)
				for _, v := range a.Values {
					n.Send(msg.LeaderID, Msg{Kind: kindForward, ForwardValue: v})
				}
			}
		}
	}

	log := getLog(n)
	logOk := len(log) >= msg.PrefixLen &&
		(msg.PrefixLen == 0 || log[msg.PrefixLen-1].Term == msg.PrefixTerm)

	if msg.LTerm == currentTerm && logOk {
		a.appendEntries(n, msg.PrefixLen, msg.LeaderCommit, msg.Suffix)
		ack := msg.PrefixLen + len(msg.Suffix)
		n.Send(msg.LeaderID, Msg{
			Kind:       kindLogResponse,
			FollowerID: n.ID(),
			FTerm:      currentTerm,
			Ack:        ack,
			Success:    true,
		})
	} else {
		n.Send(msg.LeaderID, Msg{
			Kind:       kindLogResponse,
			FollowerID: n.ID(),
			FTerm:      currentTerm,
			Ack:        0,
			Success:    false,
		})
	}
}

// ── AppendEntries ────────────────────────────────────────────────────────────

func (a *Algorithm) appendEntries(n *core.Node, prefixLen, leaderCommit int, suffix []LogEntry) {
	log := getLog(n)

	if len(suffix) > 0 && len(log) > prefixLen {
		index := min(len(log), prefixLen+len(suffix)) - 1
		if log[index].Term != suffix[index-prefixLen].Term {
			log = log[:prefixLen]
		}
	}

	if prefixLen+len(suffix) > len(log) {
		for i := len(log) - prefixLen; i < len(suffix); i++ {
			log = append(log, suffix[i])
		}
	}
	n.Set(sLog, log)

	commitLength := getCommitLength(n)
	if leaderCommit > commitLength {
		delivered := getDelivered(n)
		for i := commitLength; i < leaderCommit && i < len(log); i++ {
			delivered = append(delivered, log[i].Value)
		}
		n.Set(sDelivered, delivered)
		n.Set(sCommitLength, leaderCommit)
	}
}

// ── LogResponse ──────────────────────────────────────────────────────────────

func (a *Algorithm) handleLogResponse(n *core.Node, msg Msg) {
	currentTerm := getTerm(n)

	if msg.FTerm == currentTerm && getRole(n) == roleLeader {
		if msg.Success && msg.Ack >= getAckedLength(n)[msg.FollowerID] {
			sentLength := getSentLength(n)
			ackedLength := getAckedLength(n)
			sentLength[msg.FollowerID] = msg.Ack
			ackedLength[msg.FollowerID] = msg.Ack
			n.Set(sSentLength, sentLength)
			n.Set(sAckedLength, ackedLength)
			a.commitLogEntries(n)
		} else if getSentLength(n)[msg.FollowerID] > 0 {
			sentLength := getSentLength(n)
			sentLength[msg.FollowerID]--
			n.Set(sSentLength, sentLength)
			a.replicateLog(n, msg.FollowerID)
		}
	} else if msg.FTerm > currentTerm {
		n.Set(sCurrentTerm, msg.FTerm)
		n.Set(sCurrentRole, roleFollower)
		n.Set(sVotedFor, core.NodeID(""))
		a.resetElectionTimer(n)
	}
}

// ── CommitLogEntries ─────────────────────────────────────────────────────────

func (a *Algorithm) commitLogEntries(n *core.Node) {
	allNodes := getAllNodes(n)
	majority := (len(allNodes) + 1) / 2
	ackedLength := getAckedLength(n)
	commitLength := getCommitLength(n)
	log := getLog(n)
	currentTerm := getTerm(n)

	// Find the largest log index acknowledged by a majority.
	readyMax := 0
	for i := commitLength + 1; i <= len(log); i++ {
		count := 0
		for _, id := range allNodes {
			if ackedLength[id] >= i {
				count++
			}
		}
		if count >= majority {
			readyMax = i
		}
	}

	if readyMax > 0 && log[readyMax-1].Term == currentTerm {
		delivered := getDelivered(n)
		for i := commitLength; i < readyMax; i++ {
			delivered = append(delivered, log[i].Value)
		}
		n.Set(sDelivered, delivered)
		n.Set(sCommitLength, readyMax)

		// Replicate again so followers receive the updated leaderCommit.
		for _, nb := range n.Neighbors() {
			a.replicateLog(n, nb)
		}
	}
}

// isDone returns true if this node has committed all expected values,
// meaning the simulation can stop (no more timers needed).
func (a *Algorithm) isDone(n *core.Node) bool {
	delivered := getDelivered(n)
	return len(delivered) >= len(a.Values) && len(a.Values) > 0
}

// ── Timer helpers ────────────────────────────────────────────────────────────

func (a *Algorithm) resetElectionTimer(n *core.Node) {
	// Reset the heartbeat counter so the election timeout doesn't fire.
	n.Set(sLastHeartbeat, getTickCount(n))
}

// ── State accessors ──────────────────────────────────────────────────────────

func getTerm(n *core.Node) int64 {
	v, _ := n.Get(sCurrentTerm)
	if t, ok := v.(int64); ok {
		return t
	}
	return 0
}

func getRole(n *core.Node) role {
	v, _ := n.Get(sCurrentRole)
	if r, ok := v.(role); ok {
		return r
	}
	return roleFollower
}

func getVotedFor(n *core.Node) core.NodeID {
	v, _ := n.Get(sVotedFor)
	if id, ok := v.(core.NodeID); ok {
		return id
	}
	return ""
}

func getVotes(n *core.Node) map[core.NodeID]bool {
	v, _ := n.Get(sVotesReceived)
	if m, ok := v.(map[core.NodeID]bool); ok {
		return m
	}
	return map[core.NodeID]bool{}
}

func getLog(n *core.Node) []LogEntry {
	v, _ := n.Get(sLog)
	if l, ok := v.([]LogEntry); ok {
		return l
	}
	return nil
}

func getCommitLength(n *core.Node) int {
	v, _ := n.Get(sCommitLength)
	if c, ok := v.(int); ok {
		return c
	}
	return 0
}

func getSentLength(n *core.Node) map[core.NodeID]int {
	v, _ := n.Get(sSentLength)
	if m, ok := v.(map[core.NodeID]int); ok {
		return m
	}
	return map[core.NodeID]int{}
}

func getAckedLength(n *core.Node) map[core.NodeID]int {
	v, _ := n.Get(sAckedLength)
	if m, ok := v.(map[core.NodeID]int); ok {
		return m
	}
	return map[core.NodeID]int{}
}

func getAllNodes(n *core.Node) []core.NodeID {
	v, _ := n.Get(sAllNodes)
	if ids, ok := v.([]core.NodeID); ok {
		return ids
	}
	return nil
}

func getDelivered(n *core.Node) []any {
	v, _ := n.Get(sDelivered)
	if d, ok := v.([]any); ok {
		return d
	}
	return nil
}

func getTickCount(n *core.Node) int64 {
	v, _ := n.Get(sTickCount)
	if t, ok := v.(int64); ok {
		return t
	}
	return 0
}

func getLastHeartbeat(n *core.Node) int64 {
	v, _ := n.Get(sLastHeartbeat)
	if t, ok := v.(int64); ok {
		return t
	}
	return 0
}

// ── Verification helpers (used in tests) ─────────────────────────────────────

// Committed returns the list of values committed (delivered) by the given node.
func Committed(sim *core.Simulator, id core.NodeID) []any {
	n := sim.NodeState(id)
	if n == nil {
		return nil
	}
	v, _ := n.Get(sDelivered)
	if d, ok := v.([]any); ok {
		return d
	}
	return nil
}

// AllCommitted returns true if every non-crashed node has committed all
// expected values in the same order.
func AllCommitted(sim *core.Simulator, ids []core.NodeID, expected []any) bool {
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed {
			continue
		}
		committed := Committed(sim, id)
		if len(committed) != len(expected) {
			return false
		}
		for i, v := range expected {
			if committed[i] != v {
				return false
			}
		}
	}
	return true
}

// HasLeader returns true if at least one non-crashed node believes it is the leader.
func HasLeader(sim *core.Simulator, ids []core.NodeID) bool {
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed {
			continue
		}
		v, _ := n.Get(sCurrentRole)
		if v == roleLeader {
			return true
		}
	}
	return false
}

// LeaderID returns the NodeID of the current leader, or "" if none.
func LeaderID(sim *core.Simulator, ids []core.NodeID) core.NodeID {
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed {
			continue
		}
		v, _ := n.Get(sCurrentRole)
		if v == roleLeader {
			return id
		}
	}
	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

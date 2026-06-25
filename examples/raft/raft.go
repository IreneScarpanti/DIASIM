package raft

import (
	"fmt"

	"diasim/pkg/core"
)

// ── Timer intervals ──────────────────────────────────────────────────────────

const (
	electionBaseInterval = 15
	electionJitter       = 2
	replicationInterval  = 5
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
	kindForward      msgKind = "FORWARD"
)

type Msg struct {
	Kind msgKind

	CandidateID core.NodeID
	CTerm       int64
	CLogLength  int
	CLogTerm    int64

	VoterID core.NodeID
	VTerm   int64
	Granted bool

	LeaderID     core.NodeID
	LTerm        int64
	PrefixLen    int
	PrefixTerm   int64
	LeaderCommit int
	Suffix       []LogEntry

	FollowerID core.NodeID
	FTerm      int64
	Ack        int
	Success    bool

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

type LogEntry struct {
	Term  int64
	Value any
}

// ── State keys ───────────────────────────────────────────────────────────────

const (
	sCurrentTerm = "raft_currentTerm"
	sVotedFor    = "raft_votedFor"
	sCurrentRole = "raft_currentRole"
	// sCurrentLeader: l'ID del leader che il nodo riconosce nel term corrente.
	sCurrentLeader   = "raft_currentLeader"
	sVotesReceived   = "raft_votesReceived"
	sLog             = "raft_log"
	sCommitLength    = "raft_commitLength"
	sSentLength      = "raft_sentLength"
	sAckedLength     = "raft_ackedLength"
	sNodeIndex       = "raft_nodeIndex"
	sDelivered       = "raft_delivered"
	sAllNodes        = "raft_allNodes"
	sLastHeartbeat   = "raft_lastHeartbeat"
	sTickCount       = "raft_tickCount"
	sLastKnownLeader = "raft_lastKnownLeader"
)

// ── Algorithm ────────────────────────────────────────────────────────────────

type Algorithm struct {
	Values    []any
	Initiator core.NodeID
}

func (a *Algorithm) OnStart(n *core.Node) {
	neighbors := n.Neighbors()
	allNodes := append([]core.NodeID{n.ID()}, neighbors...)

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
	n.Set(sLastHeartbeat, int64(0))
	n.Set(sTickCount, int64(0))
	n.Set(sLastKnownLeader, core.NodeID("")) // FIX #3

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
		return
	}

	tickCount := getTickCount(n) + 1
	n.Set(sTickCount, tickCount)

	currentRole := getRole(n)

	if currentRole == roleLeader {
		for _, nb := range n.Neighbors() {
			a.replicateLog(n, nb)
		}
	} else {
		nodeIndex, _ := n.Get(sNodeIndex)
		idx, _ := nodeIndex.(int)
		electionTicks := int64(electionBaseInterval/replicationInterval) + int64(idx)
		lastHB := getLastHeartbeat(n)

		if tickCount-lastHB >= electionTicks {
			a.handleElectionTimeout(n)
		}
	}

	n.SetTimer(replicationInterval)
}

// ── Election timeout ─────────────────────────────────────────────────────────

func (a *Algorithm) handleElectionTimeout(n *core.Node) {
	currentRole := getRole(n)
	if currentRole == roleLeader {
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

			a.injectValues(n)

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
	if getRole(n) != roleLeader {
		return
	}
	// Dedup: controlla se il valore è già presente nel log.
	log := getLog(n)
	for _, entry := range log {
		if entry.Value == msg.ForwardValue {
			return
		}
	}
	a.appendAndReplicate(n, msg.ForwardValue)
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

// injectValues: chiamato quando l'initiator vince l'elezione e diventa leader.
// Aggiunge al log i valori non ancora presenti (dedup per evitare duplicati
// nel caso in cui l'initiator avesse già fatto forward in un term precedente).
func (a *Algorithm) injectValues(n *core.Node) {
	if n.ID() != a.Initiator {
		return
	}
	// Segna il leader corrente come noto, così handleLogRequest non rimanderà
	// i valori a sé stessi se l'initiator riceverà un proprio LOG_REQ.
	n.Set(sLastKnownLeader, n.ID())

	// Dedup: aggiungi solo i valori non ancora nel log.
	log := getLog(n)
	existing := make(map[interface{}]bool, len(log))
	for _, entry := range log {
		existing[entry.Value] = true
	}
	for _, v := range a.Values {
		if !existing[v] {
			a.appendAndReplicate(n, v)
		}
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

// FIX #3 (parte follower): l'initiator riinvia i valori non committati ogni
// volta che scopre un leader diverso dall'ultimo a cui ha già fatto forward.
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

		// FIX #3: usa sLastKnownLeader invece del flag booleano sValuesInjected.
		// Se il nodo initiator rileva un cambio di leader, rispedisce i valori
		// non ancora committati. La dedup in handleForward impedisce duplicati.
		if n.ID() == a.Initiator {
			lastLeader := getNodeID(n, sLastKnownLeader)
			if lastLeader != msg.LeaderID {
				n.Set(sLastKnownLeader, msg.LeaderID)
				commitLength := getCommitLength(n)
				if commitLength < len(a.Values) {
					for _, v := range a.Values[commitLength:] {
						n.Send(msg.LeaderID, Msg{Kind: kindForward, ForwardValue: v})
					}
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

		for _, nb := range n.Neighbors() {
			a.replicateLog(n, nb)
		}
	}
}

func (a *Algorithm) isDone(n *core.Node) bool {
	delivered := getDelivered(n)
	return len(delivered) >= len(a.Values) && len(a.Values) > 0
}

// ── Timer helpers ────────────────────────────────────────────────────────────

func (a *Algorithm) resetElectionTimer(n *core.Node) {
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

func getNodeID(n *core.Node, key string) core.NodeID {
	v, _ := n.Get(key)
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

// ── Verification helpers ─────────────────────────────────────────────────────

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

package bully

import (
	"fmt"
	"strconv"
	"strings"

	"diasim/pkg/core"
)

// ── Timer intervals ──────────────────────────────────────────────────────────

const (
	heartbeatInterval   = 5
	electionTimeout     = 20
	electionWaitTimeout = 10
	tickInterval        = 3
)

// ── Message types ────────────────────────────────────────────────────────────

type msgKind string

const (
	kindElection    msgKind = "ELECTION"
	kindAnswer      msgKind = "ANSWER"
	kindCoordinator msgKind = "COORDINATOR"
	kindHeartbeat   msgKind = "HEARTBEAT"
	kindReplicate   msgKind = "REPLICATE"
	kindAck         msgKind = "ACK"
	kindForward     msgKind = "FORWARD"
)

// Msg is the payload for all bully messages.
type Msg struct {
	Kind     msgKind
	From     core.NodeID
	Priority int
	Value    any
	Values   []any // log completo inviato dal leader
	Index    int   // value index for ACK
	// FIX #2: il leader comunica il proprio commit index ai follower,
	// così i follower consegnano solo valori effettivamente committati.
	LeaderCommit int
}

func (m Msg) String() string {
	switch m.Kind {
	case kindElection:
		return fmt.Sprintf("ELECTION(from=%s,pri=%d)", m.From, m.Priority)
	case kindAnswer:
		return fmt.Sprintf("ANSWER(from=%s,pri=%d)", m.From, m.Priority)
	case kindCoordinator:
		return fmt.Sprintf("COORDINATOR(from=%s,pri=%d)", m.From, m.Priority)
	case kindHeartbeat:
		return fmt.Sprintf("HEARTBEAT(leader=%s)", m.From)
	case kindReplicate:
		return fmt.Sprintf("REPLICATE(leader=%s,vals=%d,commit=%d)", m.From, len(m.Values), m.LeaderCommit)
	case kindAck:
		return fmt.Sprintf("ACK(from=%s,idx=%d)", m.From, m.Index)
	case kindForward:
		return fmt.Sprintf("FORWARD(val=%v)", m.Value)
	default:
		return fmt.Sprintf("BullyMsg(%s)", m.Kind)
	}
}

// ── State keys ───────────────────────────────────────────────────────────────

const (
	sPriority       = "bully_priority"
	sLeaderID       = "bully_leaderID"
	sLeaderPriority = "bully_leaderPri"
	sElecting       = "bully_electing"
	sGotAnswer      = "bully_gotAnswer"
	sTickCount      = "bully_tickCount"
	sLastHeartbeat  = "bully_lastHeartbeat"
	sElectionStart  = "bully_electionStart"
	sAllNodes       = "bully_allNodes"
	sDelivered      = "bully_delivered"
	sValuesInjected = "bully_valuesInjected"
	sPendingValues  = "bully_pendingValues"
	sReplicatedVals = "bully_replicatedVals"
	sAckCounts      = "bully_ackCounts"
	sCommitIndex    = "bully_commitIndex"
	sIsLeader       = "bully_isLeader"
	// FIX #2: log locale del follower, separato da sDelivered.
	// sDelivered contiene solo i valori che il leader ha committato (LeaderCommit),
	// sLocalLog contiene tutti i valori ricevuti (non-committati inclusi).
	sLocalLog = "bully_localLog"
)

// ── Algorithm ────────────────────────────────────────────────────────────────

type Algorithm struct {
	Values    []any
	Initiator core.NodeID
}

func priority(id core.NodeID) int {
	s := string(id)
	s = strings.TrimLeft(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_")
	p, _ := strconv.Atoi(s)
	return p
}

func (a *Algorithm) OnStart(n *core.Node) {
	allNodes := append([]core.NodeID{n.ID()}, n.Neighbors()...)
	pri := priority(n.ID())

	n.Set(sPriority, pri)
	n.Set(sLeaderID, core.NodeID(""))
	n.Set(sLeaderPriority, -1)
	n.Set(sElecting, false)
	n.Set(sGotAnswer, false)
	n.Set(sTickCount, int64(0))
	n.Set(sLastHeartbeat, int64(0))
	n.Set(sElectionStart, int64(0))
	n.Set(sAllNodes, allNodes)
	n.Set(sDelivered, []any{})
	n.Set(sLocalLog, []any{}) // FIX #2
	n.Set(sValuesInjected, false)
	n.Set(sPendingValues, []any{})
	n.Set(sReplicatedVals, []any{})
	n.Set(sAckCounts, make(map[int]map[core.NodeID]bool))
	n.Set(sCommitIndex, 0)
	n.Set(sIsLeader, false)

	a.startElection(n)
	n.SetTimer(tickInterval)
}

func (a *Algorithm) OnMessage(n *core.Node, msg core.Message) {
	payload, ok := msg.Payload.(Msg)
	if !ok {
		return
	}

	switch payload.Kind {
	case kindElection:
		a.handleElection(n, payload)
	case kindAnswer:
		a.handleAnswer(n, payload)
	case kindCoordinator:
		a.handleCoordinator(n, payload)
	case kindHeartbeat:
		a.handleHeartbeat(n, payload)
	case kindReplicate:
		a.handleReplicate(n, payload)
	case kindAck:
		a.handleAck(n, payload)
	case kindForward:
		a.handleForward(n, payload)
	}
}

func (a *Algorithm) OnTick(n *core.Node) {
	if a.isDone(n) {
		return
	}

	tc := getInt64(n, sTickCount) + 1
	n.Set(sTickCount, tc)

	isLeader := getBool(n, sIsLeader)

	if isLeader {
		a.leaderTick(n)
	} else {
		electing := getBool(n, sElecting)
		if electing {
			gotAnswer := getBool(n, sGotAnswer)
			elStart := getInt64(n, sElectionStart)
			if gotAnswer && (tc-elStart) > electionWaitTimeout {
				a.startElection(n)
			}
			if !gotAnswer && (tc-elStart) > electionWaitTimeout {
				a.becomeLeader(n)
			}
		} else {
			lastHB := getInt64(n, sLastHeartbeat)
			if (tc - lastHB) > electionTimeout {
				a.startElection(n)
			}
		}
	}

	n.SetTimer(tickInterval)
}

// ── Election logic ───────────────────────────────────────────────────────────

func (a *Algorithm) startElection(n *core.Node) {
	pri := getInt(n, sPriority)
	tc := getInt64(n, sTickCount)

	n.Set(sElecting, true)
	n.Set(sGotAnswer, false)
	n.Set(sElectionStart, tc)

	hasHigher := false
	for _, nb := range n.Neighbors() {
		if priority(nb) > pri {
			n.Send(nb, Msg{Kind: kindElection, From: n.ID(), Priority: pri})
			hasHigher = true
		}
	}

	if !hasHigher {
		a.becomeLeader(n)
	}
}

// FIX #1 – handleElection non deve innescare startElection se il nodo è già
// leader (sIsLeader=true) oppure conosce già un leader valido (sLeaderID != "").
//
// PROBLEMA ORIGINALE: quando il leader N30 riceveva messaggi ELECTION dai nodi
// a priorità inferiore (che li inviavano prima di ricevere il COORDINATOR),
// chiamava startElection → becomeLeader e rispediva COORDINATOR a tutti i 30
// nodi. Ciascuno di quei nodi, ricevendo il COORDINATOR, resettava sElecting=false;
// poi, alla ricezione del prossimo ELECTION da un nodo ancora inferiore, chiamava
// a sua volta startElection, inviando un nuovo ELECTION a N30, che lo gestiva
// con un altro becomeLeader... Il risultato era una cascata esponenziale di
// COORDINATOR che impediva alla simulazione di terminare entro i 50.000 step.
func (a *Algorithm) handleElection(n *core.Node, msg Msg) {
	pri := getInt(n, sPriority)

	if pri > msg.Priority {
		// Rispondi sempre: il mittente deve sapere che esiste un nodo vivo
		// con priorità superiore.
		n.Send(msg.From, Msg{Kind: kindAnswer, From: n.ID(), Priority: pri})

		// Avvia una nuova elezione solo se:
		//   1. non siamo già in fase di elezione,
		//   2. non siamo già il leader,
		//   3. non conosciamo ancora un leader (leader ID vuoto).
		// Senza queste guardie il leader (o qualsiasi nodo che conosce già
		// il leader) innescava elezioni superflue a ogni ELECTION ricevuto,
		// creando la cascata descritta sopra.
		knownLeader := getNodeID(n, sLeaderID)
		if !getBool(n, sElecting) && !getBool(n, sIsLeader) && knownLeader == "" {
			a.startElection(n)
		}
	}
}

func (a *Algorithm) handleAnswer(n *core.Node, msg Msg) {
	n.Set(sGotAnswer, true)
}

func (a *Algorithm) handleCoordinator(n *core.Node, msg Msg) {
	n.Set(sLeaderID, msg.From)
	n.Set(sLeaderPriority, msg.Priority)
	n.Set(sElecting, false)
	n.Set(sGotAnswer, false)
	n.Set(sIsLeader, false)
	n.Set(sLastHeartbeat, getInt64(n, sTickCount))

	if n.ID() == a.Initiator && len(a.Values) > 0 {
		delivered := getDelivered(n)
		if len(delivered) < len(a.Values) {
			for _, v := range a.Values[len(delivered):] {
				n.Send(msg.From, Msg{Kind: kindForward, From: n.ID(), Value: v})
			}
			n.Set(sValuesInjected, true)
		}
	}
}

func (a *Algorithm) becomeLeader(n *core.Node) {
	pri := getInt(n, sPriority)

	n.Set(sIsLeader, true)
	n.Set(sElecting, false)
	n.Set(sGotAnswer, false)
	n.Set(sLeaderID, n.ID())
	n.Set(sLeaderPriority, pri)

	for _, nb := range n.Neighbors() {
		n.Send(nb, Msg{Kind: kindCoordinator, From: n.ID(), Priority: pri})
	}

	if n.ID() == a.Initiator && len(a.Values) > 0 {
		delivered := getDelivered(n)
		if len(delivered) < len(a.Values) {
			pending := getPendingValues(n)
			for _, v := range a.Values[len(delivered):] {
				pending = append(pending, v)
			}
			n.Set(sPendingValues, pending)
			n.Set(sValuesInjected, true)
		}
	}
}

// ── Leader logic ─────────────────────────────────────────────────────────────

// FIX #2 (parte leader): il leader include il proprio commitIndex nel messaggio
// REPLICATE in modo che i follower sappiano fino a quale indice i valori sono
// stati confermati dalla maggioranza e possano consegnarli in sicurezza.
func (a *Algorithm) leaderTick(n *core.Node) {
	for _, nb := range n.Neighbors() {
		n.Send(nb, Msg{Kind: kindHeartbeat, From: n.ID()})
	}

	pending := getPendingValues(n)
	replicated := getReplicatedVals(n)
	commitIdx := getInt(n, sCommitIndex)

	if len(pending) > len(replicated) {
		newVals := pending[len(replicated):]
		replicated = append(replicated, newVals...)
		n.Set(sReplicatedVals, replicated)

		for _, nb := range n.Neighbors() {
			n.Send(nb, Msg{
				Kind:         kindReplicate,
				From:         n.ID(),
				Values:       replicated,
				LeaderCommit: commitIdx, // FIX #2
			})
		}
	} else if len(replicated) > 0 && commitIdx < len(replicated) {
		// Re-replica finché non abbiamo il commit completo; include sempre
		// il commitIndex corrente così i follower si aggiornano.
		for _, nb := range n.Neighbors() {
			n.Send(nb, Msg{
				Kind:         kindReplicate,
				From:         n.ID(),
				Values:       replicated,
				LeaderCommit: commitIdx, // FIX #2
			})
		}
	}
}

func (a *Algorithm) handleForward(n *core.Node, msg Msg) {
	if !getBool(n, sIsLeader) {
		return
	}
	pending := getPendingValues(n)
	replicated := getReplicatedVals(n)
	for _, existing := range pending {
		if existing == msg.Value {
			return
		}
	}
	for _, existing := range replicated {
		if existing == msg.Value {
			return
		}
	}
	pending = append(pending, msg.Value)
	n.Set(sPendingValues, pending)
}

func (a *Algorithm) handleAck(n *core.Node, msg Msg) {
	if !getBool(n, sIsLeader) {
		return
	}

	ackCounts := getAckCounts(n)
	if ackCounts[msg.Index] == nil {
		ackCounts[msg.Index] = make(map[core.NodeID]bool)
	}
	ackCounts[msg.Index][msg.From] = true
	n.Set(sAckCounts, ackCounts)

	allNodes := getAllNodes(n)
	majority := len(allNodes)/2 + 1
	commitIdx := getInt(n, sCommitIndex)
	replicated := getReplicatedVals(n)

	for i := commitIdx; i < len(replicated); i++ {
		count := len(ackCounts[i]) + 1
		if count >= majority {
			commitIdx = i + 1
		} else {
			break
		}
	}

	if commitIdx > getInt(n, sCommitIndex) {
		n.Set(sCommitIndex, commitIdx)
		delivered := getDelivered(n)
		for i := len(delivered); i < commitIdx; i++ {
			delivered = append(delivered, replicated[i])
		}
		n.Set(sDelivered, delivered)

		// FIX #2b – notifica immediata ai follower del nuovo commit index.
		//
		// PROBLEMA: il leader aggiorna sDelivered qui (in OnMessage), rendendo
		// isDone()=true. Il successivo OnTick ritorna subito senza chiamare
		// leaderTick, quindi nessun REPLICATE con LeaderCommit aggiornato viene
		// mai inviato. I follower restano in attesa di LeaderCommit > 0 per
		// sempre, e la simulazione non termina mai.
		//
		// FIX: broadcast immediato con il commitIdx appena raggiunto, senza
		// aspettare il prossimo tick. Questo garantisce che i follower ricevano
		// la notifica di commit anche quando il leader si ferma subito dopo.
		for _, nb := range n.Neighbors() {
			n.Send(nb, Msg{
				Kind:         kindReplicate,
				From:         n.ID(),
				Values:       replicated,
				LeaderCommit: commitIdx,
			})
		}
	}
}

// ── Follower logic ───────────────────────────────────────────────────────────

func (a *Algorithm) handleHeartbeat(n *core.Node, msg Msg) {
	tc := getInt64(n, sTickCount)
	n.Set(sLastHeartbeat, tc)

	currentLeader := getNodeID(n, sLeaderID)
	if currentLeader == "" || currentLeader == msg.From {
		n.Set(sLeaderID, msg.From)
		n.Set(sElecting, false)
	}
}

// FIX #2 (parte follower): i follower mantengono un log locale (sLocalLog)
// separato dai valori consegnati (sDelivered).
//
// PROBLEMA ORIGINALE: i follower aggiungevano ogni valore ricevuto direttamente
// a sDelivered prima che il leader raggiungesse la maggioranza. Questo viola
// la safety: se il leader crashava dopo aver replicato a meno della maggioranza,
// nodi diversi avrebbero potuto consegnare valori diversi. Il sintomo visibile
// era values_committed > 0 anche quando f ≥ 16 (impossibile raggiungere la
// maggioranza), dando l'impressione erronea di successo parziale.
//
// FIX: il follower:
//  1. Aggiunge i nuovi valori a sLocalLog e manda ACK (per permettere al
//     leader di calcolare la maggioranza).
//  2. Consegna (sDelivered) solo fino a msg.LeaderCommit, che rappresenta
//     l'indice fino al quale il leader ha ricevuto la maggioranza degli ACK.
func (a *Algorithm) handleReplicate(n *core.Node, msg Msg) {
	tc := getInt64(n, sTickCount)
	n.Set(sLastHeartbeat, tc)
	n.Set(sLeaderID, msg.From)
	n.Set(sElecting, false)

	// 1. Aggiorna il log locale e manda ACK per ogni nuova entry.
	localLog := getLocalLog(n)
	for i := len(localLog); i < len(msg.Values); i++ {
		localLog = append(localLog, msg.Values[i])
		n.Send(msg.From, Msg{Kind: kindAck, From: n.ID(), Index: i})
	}
	n.Set(sLocalLog, localLog)

	// 2. Consegna solo i valori che il leader ha già committato.
	delivered := getDelivered(n)
	for i := len(delivered); i < msg.LeaderCommit && i < len(localLog); i++ {
		delivered = append(delivered, localLog[i])
	}
	n.Set(sDelivered, delivered)
}

// ── Termination ──────────────────────────────────────────────────────────────

func (a *Algorithm) isDone(n *core.Node) bool {
	if len(a.Values) == 0 {
		return getNodeID(n, sLeaderID) != ""
	}
	delivered := getDelivered(n)
	return len(delivered) >= len(a.Values)
}

// ── State helpers ────────────────────────────────────────────────────────────

func getInt(n *core.Node, key string) int {
	v, _ := n.Get(key)
	if i, ok := v.(int); ok {
		return i
	}
	return 0
}

func getInt64(n *core.Node, key string) int64 {
	v, _ := n.Get(key)
	if i, ok := v.(int64); ok {
		return i
	}
	return 0
}

func getBool(n *core.Node, key string) bool {
	v, _ := n.Get(key)
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func getNodeID(n *core.Node, key string) core.NodeID {
	v, _ := n.Get(key)
	if id, ok := v.(core.NodeID); ok {
		return id
	}
	return ""
}

func getAllNodes(n *core.Node) []core.NodeID {
	v, _ := n.Get(sAllNodes)
	if ns, ok := v.([]core.NodeID); ok {
		return ns
	}
	return nil
}

func getDelivered(n *core.Node) []any {
	v, _ := n.Get(sDelivered)
	if d, ok := v.([]any); ok {
		return d
	}
	return []any{}
}

func getLocalLog(n *core.Node) []any {
	v, _ := n.Get(sLocalLog)
	if l, ok := v.([]any); ok {
		return l
	}
	return []any{}
}

func getPendingValues(n *core.Node) []any {
	v, _ := n.Get(sPendingValues)
	if p, ok := v.([]any); ok {
		return p
	}
	return []any{}
}

func getReplicatedVals(n *core.Node) []any {
	v, _ := n.Get(sReplicatedVals)
	if r, ok := v.([]any); ok {
		return r
	}
	return []any{}
}

func getAckCounts(n *core.Node) map[int]map[core.NodeID]bool {
	v, _ := n.Get(sAckCounts)
	if m, ok := v.(map[int]map[core.NodeID]bool); ok {
		return m
	}
	return make(map[int]map[core.NodeID]bool)
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
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
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
		v, _ := n.Get(sIsLeader)
		if v == true {
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
		v, _ := n.Get(sIsLeader)
		if v == true {
			return id
		}
	}
	return ""
}

func AgreedLeader(sim *core.Simulator, ids []core.NodeID) (core.NodeID, int, int) {
	votes := make(map[core.NodeID]int)
	total := 0
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		total++
		leader := getNodeID(n, sLeaderID)
		if leader != "" {
			votes[leader]++
		}
	}
	bestLeader := core.NodeID("")
	bestCount := 0
	for l, c := range votes {
		if c > bestCount {
			bestLeader = l
			bestCount = c
		}
	}
	return bestLeader, bestCount, total
}

func CountCorrectDeliveries(sim *core.Simulator, ids []core.NodeID, expected []any) (delivered, total int) {
	for _, id := range ids {
		n := sim.NodeState(id)
		if n == nil || n.Status() == core.StatusCrashed || n.IsByzantine() {
			continue
		}
		total++
		committed := Committed(sim, id)
		if len(committed) >= len(expected) {
			match := true
			for i, v := range expected {
				if committed[i] != v {
					match = false
					break
				}
			}
			if match {
				delivered++
			}
		}
	}
	return
}

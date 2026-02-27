package core

import "fmt"

type NodeID string

type EventType string

const (
	EventMessage     EventType = "MESSAGE"
	EventTick        EventType = "TICK"
	EventStart       EventType = "START"
	EventCrash       EventType = "CRASH"
	EventLinkFail    EventType = "LINK_FAIL"
	EventLinkRecover EventType = "LINK_RECOVER"
	EventNodeRecover EventType = "NODE_RECOVER"
)

type Message struct {
	From    NodeID
	To      NodeID
	Payload any
}

func (m Message) String() string {
	return fmt.Sprintf("Message{from=%s, to=%s, payload=%v}", m.From, m.To, m.Payload)
}

type Event struct {
	Time   int64
	Type   EventType
	NodeID NodeID
	SeqNum int64

	Msg *Message

	LinkFrom NodeID
	LinkTo   NodeID
}

func (e *Event) Less(other *Event) bool {
	if e.Time != other.Time {
		return e.Time < other.Time
	}
	return e.SeqNum < other.SeqNum
}

func (e *Event) String() string {
	switch e.Type {
	case EventMessage:
		return fmt.Sprintf("Event{t=%d, %s, node=%s, msg=%s, seq=%d}", e.Time, e.Type, e.NodeID, e.Msg, e.SeqNum)
	case EventLinkFail, EventLinkRecover:
		return fmt.Sprintf("Event{t=%d, %s, link=%s->%s, seq=%d}", e.Time, e.Type, e.LinkFrom, e.LinkTo, e.SeqNum)
	default:
		return fmt.Sprintf("Event{t=%d, %s, node=%s, seq=%d}", e.Time, e.Type, e.NodeID, e.SeqNum)
	}
}

type ActionType string

const (
	ActionSend     ActionType = "SEND"
	ActionSetTimer ActionType = "SET_TIMER"
)

type Action struct {
	Type ActionType

	SendTo  NodeID
	Payload any

	Delay int64
}

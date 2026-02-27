package core

import (
	"fmt"
	"io"
	"sync"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	default:
		return "UNKNOWN"
	}
}

type LogEntry struct {
	Time    int64
	Level   Level
	Kind    string
	NodeID  NodeID
	Details string
}

func (e LogEntry) String() string {
	node := string(e.NodeID)
	if node == "" {
		node = "SIMULATOR"
	}
	return fmt.Sprintf("t=%-4d %-5s %-14s %-10s %s",
		e.Time, e.Level, e.Kind, node, e.Details)
}

type Logger struct {
	mu      sync.Mutex
	out     io.Writer
	entries []LogEntry
	level   Level
}

func newLogger(w io.Writer, minLevel Level) *Logger {
	return &Logger{out: w, level: minLevel}
}

func (l *Logger) log(e LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, e)
	if e.Level >= l.level {
		fmt.Fprintln(l.out, e.String())
	}
}

// Entries returns a copy of all recorded log entries.
func (l *Logger) Entries() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make([]LogEntry, len(l.entries))
	copy(cp, l.entries)
	return cp
}

func (l *Logger) simStart(t int64, nodeCount int) {
	l.log(LogEntry{t, LevelInfo, "SIM_START", "", fmt.Sprintf("nodes=%d", nodeCount)})
}

func (l *Logger) simEnd(t int64, steps int) {
	l.log(LogEntry{t, LevelInfo, "SIM_END", "", fmt.Sprintf("steps=%d", steps)})
}

func (l *Logger) nodeStart(t int64, id NodeID) {
	l.log(LogEntry{t, LevelInfo, "NODE_START", id, "node initialised"})
}

func (l *Logger) nodeCrash(t int64, id NodeID) {
	l.log(LogEntry{t, LevelWarn, "NODE_CRASH", id, "node crashed"})
}

func (l *Logger) nodeRecover(t int64, id NodeID) {
	l.log(LogEntry{t, LevelInfo, "NODE_RECOVER", id, "node recovered"})
}

func (l *Logger) msgSend(t int64, from, to NodeID, payload any) {
	l.log(LogEntry{t, LevelDebug, "MSG_SEND", from,
		fmt.Sprintf("to=%s payload=%v", to, payload)})
}

func (l *Logger) msgScheduled(t int64, from, to NodeID, deliverAt int64, payload any) {
	l.log(LogEntry{t, LevelDebug, "MSG_SCHED", from,
		fmt.Sprintf("to=%s deliverAt=%d payload=%v", to, deliverAt, payload)})
}

func (l *Logger) msgDeliver(t int64, from, to NodeID, payload any) {
	l.log(LogEntry{t, LevelInfo, "MSG_DELIVER", to,
		fmt.Sprintf("from=%s payload=%v", from, payload)})
}

func (l *Logger) msgDropped(t int64, from, to NodeID, reason string) {
	l.log(LogEntry{t, LevelWarn, "MSG_DROP", to,
		fmt.Sprintf("from=%s reason=%s", from, reason)})
}

func (l *Logger) timerSet(t int64, id NodeID, fireAt int64) {
	l.log(LogEntry{t, LevelDebug, "TIMER_SET", id,
		fmt.Sprintf("fireAt=%d", fireAt)})
}

func (l *Logger) timerFire(t int64, id NodeID) {
	l.log(LogEntry{t, LevelInfo, "TIMER_FIRE", id, ""})
}

func (l *Logger) linkFail(t int64, from, to NodeID) {
	l.log(LogEntry{t, LevelWarn, "LINK_FAIL", "",
		fmt.Sprintf("link=%s->%s failed", from, to)})
}

func (l *Logger) linkRecover(t int64, from, to NodeID) {
	l.log(LogEntry{t, LevelInfo, "LINK_RECOVER", "",
		fmt.Sprintf("link=%s->%s recovered", from, to)})
}

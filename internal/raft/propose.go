package raft

// Propose Command
// Appends a command to the leader's log and waits for it to be committed.
func (r *Raft) Propose(command interface{}) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return -1, -1, false
	}

	index := r.getLastLogIndex() + 1
	term := r.currentTerm
	r.log = append(r.log, LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	})

	// Inform everyone about the new entry immediately
	r.heartbeatTimer.Reset(0)

	return index, term, true
}

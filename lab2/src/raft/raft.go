package raft

// Package raft implements the Raft consensus algorithm.
//
// This file outlines the public API that the Raft module exposes to the service (or tester).
//
// Usage:
//
//   rf := Make(...)
//     Creates a new Raft server.
//
//   rf.Start(command interface{}) (index int, term int, isLeader bool)
//     Begins agreement on a new log entry. Returns the log index, current term, and whether this server is the leader.
//
//   rf.GetState() (term int, isLeader bool)
//     Reports the current term and whether this server believes it is the leader.
//
//   ApplyMsg
//     When a new entry is committed to the log, each Raft peer should send an ApplyMsg
//     to the service (or tester) on the same server.

import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// ApplyMsg represents a message sent by a Raft peer to the service
// indicating a committed log entry.
//
// When CommandValid is true, Command contains a newly committed log entry
// and CommandIndex specifies its log index. Other types of messages may
// be sent with CommandValid set to false.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// State represents the role of a Raft server in the consensus algorithm.
type State int

const (
	// Follower is the default state. It responds to requests and awaits instructions from a leader.
	Follower State = iota

	// Candidate is a server that has timed out without hearing from a leader and is attempting to become one.
	Candidate

	// Leader is the server that manages log replication and handles client requests.
	Leader
)

// String returns the string representation of the Raft server state.
// Useful for logging and debugging to display the current role
// as "Follower", "Candidate", or "Leader".
func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Raft represents a single Raft peer in the cluster.
type Raft struct {
	mu        sync.Mutex          // Mutex to protect concurrent access to this peer's state.
	peers     []*labrpc.ClientEnd // RPC endpoints of all peers.
	persister *Persister          // Object to manage this peer's persisted state.
	me        int                 // Index of this peer in the peers slice.

	// Persistent state on all servers (updated on stable storage before responding to RPCs).
	currentTerm int        // Latest term server has seen.
	votedFor    int        // Candidate ID that received vote in current term (or -1 if none).
	log         []LogEntry // Log entries; each contains command for state machine and term when entry was received.

	// Volatile state on all servers.
	commitIndex int // Index of highest log entry known to be committed.
	lastApplied int // Index of highest log entry applied to state machine.

	// Volatile state on leaders (reinitialized after election).
	nextIndex  []int // For each server, index of the next log entry to send to that server.
	matchIndex []int // For each server, index of highest log entry known to be replicated on server.

	// Communication channel to send committed log entries to the service.
	applyCh chan ApplyMsg

	// Current role/state of the server: Follower, Candidate, or Leader.
	state State

	// Timestamp of the last received heartbeat or valid RPC from a leader.
	lastHeartBeat time.Time
}

// GetState reports the current term and whether this Raft server
// believes it is the leader. Useful for clients to determine the
// current leader status.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// persist saves the Raft server's currentTerm, votedFor, and log to stable storage.
// This allows the server to recover its state after a crash or restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// readPersist restores the previously persisted state from stable storage.
// This includes currentTerm, votedFor, and the log. Called after a restart.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // no persisted state to restore
		return
	}

	// Create a decoder to read the persisted byte slice
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	// Temporary variables to decode into
	var currentTerm int
	var votedFor int
	var log []LogEntry

	// Attempt to decode each persisted field
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// Decoding failed — log an error for debugging
		DPrintf("[S%d] Failed to decode persistent state", rf.me)
	}

	// Restore the state from decoded values
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
}

// RequestVoteArgs defines the arguments for a RequestVote RPC.
// Sent by a candidate to gather votes from other servers.
type RequestVoteArgs struct {
	Term         int // Candidate’s term
	CandidateId  int // Candidate requesting vote
	LastLogIndex int // Index of candidate’s last log entry
	LastLogTerm  int // Term of candidate’s last log entry
}

// RequestVoteReply defines the reply for a RequestVote RPC.
type RequestVoteReply struct {
	Term        int  // Current term, for candidate to update itself
	VoteGranted bool // True if candidate received vote
}

// LogEntry represents a single command entry in the Raft log.
type LogEntry struct {
	Command interface{} // Command for the state machine
	Term    int         // Term when entry was received by leader
}

// AppendEntriesArgs defines the arguments for an AppendEntries RPC.
// Used for heartbeats and log replication by the leader.
type AppendEntriesArgs struct {
	Term         int        // Leader’s term
	LeaderId     int        // Leader ID to redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader’s commit index
}

// AppendEntriesReply defines the reply for an AppendEntries RPC.
type AppendEntriesReply struct {
	Term          int  // Current term, for leader to update itself
	Success       bool // True if follower contained matching PrevLogIndex and PrevLogTerm
	ConflictTerm  int  // Term of conflicting entry (if any)
	ConflictIndex int  // Index of first entry with ConflictTerm (or next expected index)
}

// RequestVote handles incoming RequestVote RPCs from candidates.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reject the vote if the candidate's term is older than the current term
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// If the candidate's term is newer, update current term and step down to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// Determine if the candidate's log is at least as up to date as receiver's log
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogTerm(lastLogIndex)

	upToDate := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	// Grant vote if this server hasn't voted yet or has already voted for the candidate,
	// and the candidate's log is sufficiently up to date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
	} else {
		reply.VoteGranted = false
	}

	// Always reply with the current term
	reply.Term = rf.currentTerm
}

// sendRequestVote sends a RequestVote RPC to the given server.
// It returns true if the RPC was successfully delivered and a response was received.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	// If the RPC call was successful, process the reply
	if ok == true {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// If the reply's term is newer, update local term and convert to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
		}
	}

	return ok
}

// min returns the smaller of two integers.
// Used in log replication to compute the new commitIndex based on the leader's commit index
// and the index of the last new log entry (e.g., in AppendEntries).
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// AppendEntries handles incoming AppendEntries RPCs from the leader.
// It is used both for heartbeats and log replication, and helps maintain consistency across the cluster.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set the reply term to currentTerm for the leader to compare
	reply.Term = rf.currentTerm

	// 1. Reply false if the term in the request is outdated
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 2. Update local term and convert to follower if leader has a newer term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	// Reset heartbeat timer on valid AppendEntries
	rf.lastHeartBeat = time.Now()

	// 3. Reject if PrevLogIndex is out of bounds
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	// 4. Reject if log term at PrevLogIndex does not match
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		conflictTerm := rf.log[args.PrevLogIndex].Term
		reply.ConflictTerm = conflictTerm
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != conflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
			if i == 0 {
				reply.ConflictIndex = 1
			}
		}
		return
	}

	// 5. Append new entries, truncating conflicting ones first if needed
	newIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		if newIndex+i < len(rf.log) {
			if rf.log[newIndex+i].Term != entry.Term {
				if newIndex+i <= rf.commitIndex {
					reply.Success = false
					return
				}
				rf.log = rf.log[:newIndex+i]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
		}
	}

	// 6. Update commitIndex if leaderCommit is ahead
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		newCommitIndex := min(args.LeaderCommit, lastNewIndex)
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.persist()
		}
	}

	// AppendEntries was successful
	reply.Success = true
}

// getLogTerm returns the term of the log entry at the specified index.
// Returns -1 if the index is out of bounds.
// Used in RequestVote, AppendEntries, and during leader commit index updates
// (e.g., in sendAppendEntries and election) to check log consistency.
func (rf *Raft) getLogTerm(index int) int {
	if index < 0 || index >= len(rf.log) {
		return -1
	}
	return rf.log[index].Term
}

// sendAppendEntries sends an AppendEntries RPC to a follower and handles the reply.
// This function is used by the leader to replicate log entries or send heartbeats.
// It also updates matchIndex and nextIndex for the follower and tries to advance commitIndex if possible.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		// -------------------------------
		// Successful log replication
		// -------------------------------

		// Update matchIndex and nextIndex for the follower
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// Try to advance commitIndex if a majority have replicated a new entry
		if rf.state == Leader {
			for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
				count := 1 // include self
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me && rf.matchIndex[i] >= N {
						count++
					}
				}
				// Only commit entries from current term
				if count > len(rf.peers)/2 && rf.getLogTerm(N) == rf.currentTerm {
					rf.commitIndex = N
					break
				}
			}
		}
	} else {
		// -------------------------------
		// Failed replication – handle conflicts
		// -------------------------------

		// Use conflictTerm and conflictIndex to quickly roll back nextIndex
		if reply.ConflictTerm != 0 {
			index := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					index = i
					break
				}
			}
			if index >= 0 {
				rf.nextIndex[server] = index + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		} else {
			// Fallback if conflictTerm is not provided
			rf.nextIndex[server] = reply.ConflictIndex
		}
	}

	// -------------------------------
	// Cap nextIndex to avoid out-of-bounds access
	// -------------------------------
	if rf.nextIndex[server] > rf.getLastLogIndex()+1 {
		rf.nextIndex[server] = rf.getLastLogIndex() + 1
	}

	return ok
}

// HeartBeat sends AppendEntries RPCs (heartbeat or log replication) to all followers.
// This function is called periodically by the leader to maintain authority
// and to replicate log entries when needed.
func (rf *Raft) HeartBeat() {
	rf.mu.Lock()
	// Capture current leader state to use outside of lock
	currentTerm := rf.currentTerm
	leaderId := rf.me
	commitIndex := rf.commitIndex
	rf.mu.Unlock()

	// Send heartbeat or log entries to each peer
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				rf.mu.Lock()

				// -------------------------------
				// Prepare AppendEntries RPC args
				// -------------------------------

				prevLogIndex := rf.nextIndex[peer] - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
					prevLogTerm = rf.log[prevLogIndex].Term
				}

				entries := make([]LogEntry, 0)
				if rf.nextIndex[peer] <= rf.getLastLogIndex() {
					// Send log entries if there are any pending for replication
					entries = append(entries, rf.log[rf.nextIndex[peer]:]...)
				}

				args := &AppendEntriesArgs{
					currentTerm,
					leaderId,
					prevLogIndex,
					prevLogTerm,
					entries,
					commitIndex,
				}

				rf.mu.Unlock()

				// -------------------------------
				// Send AppendEntries RPC
				// -------------------------------
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(peer, args, reply)
			}(i)
		}
	}
}

// getLastLogIndex returns the index of the last log entry.
// Used in AppendEntries, election, HeartBeat, Start, and sendAppendEntries
// to determine the most recent log index for replication or comparison.
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// Start is called by the service (e.g., a key/value server) to initiate
// consensus on a new command to be appended to the Raft log.
// If the current server is not the leader, it returns false.
// If it is the leader, it appends the command to its log and begins replication.
// There is no guarantee the command will eventually be committed (e.g., due to leadership changes).
//
// Returns:
// - the index where the command will be placed (if committed),
// - the current term,
// - and a bool indicating if this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// -------------------------------
	// Return immediately if not leader
	// -------------------------------
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	isLeader := (rf.state == Leader)

	// -------------------------------
	// Append the new command to the local log
	// -------------------------------
	log_entry := LogEntry{command, rf.currentTerm}
	rf.log = append(rf.log, log_entry)
	rf.persist()

	// -------------------------------
	// Send AppendEntries RPCs to all peers
	// -------------------------------
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				term := rf.currentTerm
				leaderId := rf.me
				commitIndex := rf.commitIndex

				// Prepare prevLog information
				prevLogIndex := rf.nextIndex[peer] - 1
				prevLogTerm := 0
				if (prevLogIndex >= 0) && (prevLogIndex < len(rf.log)) {
					prevLogTerm = rf.log[prevLogIndex].Term
				}

				// Prepare log entries to send
				var entries []LogEntry
				if rf.nextIndex[peer] <= len(rf.log) {
					entries = make([]LogEntry, len(rf.log[rf.nextIndex[peer]:]))
					copy(entries, rf.log[rf.nextIndex[peer]:])
				} else {
					entries = []LogEntry{} // fallback: send empty slice
				}

				// Create AppendEntriesArgs
				args := &AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, commitIndex}
				rf.mu.Unlock()

				// Send AppendEntries RPC
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(peer, args, reply)
			}(i)
		}
	}

	// -------------------------------
	// Return the index, term, and leadership status
	// -------------------------------
	index = rf.getLastLogIndex()
	term = rf.currentTerm

	return index, term, isLeader
}

// Kill is called when the Raft instance is no longer needed.
// Currently unimplemented, but can be used to shut down goroutines or disable logging for testing.
func (rf *Raft) Kill() {
	// No-op: optional cleanup logic can be added here if needed.
}

// election initiates a new election for this Raft server.
// It increments the current term, transitions to Candidate state, votes for itself,
// and sends RequestVote RPCs to all other peers. If it receives a majority of votes,
// it becomes the leader and initializes leader-specific state.
func (rf *Raft) election() {
	rf.mu.Lock()

	// -------------------------------
	// Transition to Candidate state
	// -------------------------------
	rf.votedFor = rf.me
	rf.currentTerm += 1
	rf.persist()
	currentTerm := rf.currentTerm
	candidateId := rf.me
	rf.state = Candidate
	rf.lastHeartBeat = time.Now()
	rf.mu.Unlock()

	vote := 1
	var mu sync.Mutex

	// -------------------------------
	// Send RequestVote RPCs to peers
	// -------------------------------
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				args := &RequestVoteArgs{}
				reply := &RequestVoteReply{}
				args.Term = currentTerm
				args.CandidateId = candidateId
				args.LastLogIndex = rf.getLastLogIndex()
				args.LastLogTerm = rf.getLogTerm(args.LastLogIndex)

				rf.sendRequestVote(peer, args, reply)

				// -------------------------------
				// Handle term updates from reply
				// -------------------------------
				if reply.Term > currentTerm {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.persist()
					}
					rf.mu.Unlock()
					return
				}

				// -------------------------------
				// Count vote if granted
				// -------------------------------
				if reply.VoteGranted == true {
					mu.Lock()
					vote += 1

					// -------------------------------
					// Become leader if majority achieved
					// -------------------------------
					if vote > (len(rf.peers) / 2) {
						rf.mu.Lock()
						if (rf.state == Candidate) && (rf.currentTerm == currentTerm) {
							rf.state = Leader
							rf.persist()
						}

						// Initialize leader state
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.peers {
							rf.nextIndex[i] = rf.getLastLogIndex() + 1
							rf.matchIndex[i] = 0
							if i == rf.me {
								rf.matchIndex[i] = rf.getLastLogIndex() // Add this line
							}
						}
						rf.mu.Unlock()
					}
					mu.Unlock()
				}
			}(i)
		}
	}
}

// Make creates a new Raft server instance.
// - peers[] contains the RPC endpoints of all servers in the cluster.
// - me is the index of this server in peers[].
// - persister handles saving and restoring persistent state across restarts.
// - applyCh is the channel on which committed log entries are sent to the service.
//
// This function initializes all server state and starts background goroutines
// for handling elections, heartbeats, and applying committed entries.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// -------------------------------
	// Initialize persistent and volatile state
	// -------------------------------
	rf.applyCh = applyCh
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// Restore from persisted state if available
	rf.readPersist(persister.ReadRaftState())

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0 // add this line for clarity and safety
	}

	rf.lastHeartBeat = time.Now()

	// -------------------------------
	// Start background goroutine for elections and heartbeats
	// -------------------------------
	go func() {
		for {
			rf.mu.Lock()
			last := rf.lastHeartBeat
			state := rf.state
			rf.mu.Unlock()

			// Randomized election timeout
			duration := 400 + rand.Float64()*(650-400)
			timeout := time.Duration(duration) * time.Millisecond

			if (state != Leader) && (time.Since(last) >= timeout) {
				rf.election()
			}
			if state == Leader {
				rf.HeartBeat()
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// -------------------------------
	// Start background goroutine for applying committed entries
	// -------------------------------
	go func() {
		for {
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			lastApplied := rf.lastApplied
			log := make([]LogEntry, len(rf.log))
			copy(log, rf.log)
			rf.mu.Unlock()

			for i := lastApplied + 1; i <= commitIndex; i++ {
				if i < len(log) {
					msg := ApplyMsg{
						CommandValid: true,
						Command:      log[i].Command,
						CommandIndex: i,
					}
					rf.applyCh <- msg

					rf.mu.Lock()
					if i > rf.lastApplied {
						rf.lastApplied = i
					}
					rf.mu.Unlock()
				}
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	return rf
}

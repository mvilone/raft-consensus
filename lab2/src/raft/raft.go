package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Hint: You can organize the variables into 4 categories:
	// 1) persistent state on all servers
	// 2) volatile state on all server
	// 3) volatile state on leaders
	// 4) Channles for rafts to communicate

	//1) persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	//2) volatile state on all servers.
	commitIndex int
	lastApplied int

	//3) volatile state on leaders
	nextIndex  []int
	matchIndex []int

	//4)channels for rafts to communicate
	applyCh chan ApplyMsg

	//state of server
	state State

	//last heart beat
	lastHeartBeat time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// Hint: paper's Figure 2, RequestVote RPC, Arguments.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// Hint: paper's Figure 2, RequestVote RPC, Results.
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	// your data here (2A, 2B).
	// Hint: paper's Figure 2, AppendEntries RPC, Arguments.
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// example AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// your data here (2A, 2B).
	// Hint: paper's Figure 2, AppendEntries RPC, Results.
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Hint: use lock!
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	if (rf.votedFor == -1) || (rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// Your code here (2A)
	// Hint: Think about different states, term, how to deal with the reply.
	if ok == true {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
		}
	}

	return ok
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// example AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	newIndex := args.PrevLogIndex + 1
	j := 0
	for ; j < len(args.Entries); j++ {
		i := newIndex + j
		if i >= len(rf.log) {
			break
		}
		if rf.log[i].Term != args.Entries[j].Term {
			// Conflict found – truncate everything from this point
			rf.log = rf.log[:i]
			break
		}
	}

	//Append any new entries
	rf.log = append(rf.log, args.Entries[j:]...)

	lastNewIndex := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
	}

	if args.Term >= rf.currentTerm {
		rf.lastHeartBeat = time.Now()
	}
	reply.Success = true

}

func (rf *Raft) getLogTerm(index int) int {
	if index < 0 || index >= len(rf.log) {
		return -1
	}
	return rf.log[index].Term
}

// example AppendEntries RPC handler
// sendAppendEntries sends AppendEntries RPCs to followers and processes the replies
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// Leader attempts to advance commitIndex
		if rf.state == Leader {
			for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
				count := 1 // Count self
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me && rf.matchIndex[i] >= N {
						count++
					}
				}
				if count > len(rf.peers)/2 && rf.getLogTerm(N) == rf.currentTerm {
					rf.commitIndex = N
					break
				}
			}
		}
	} else {
		// Decrement nextIndex if replication failed (minimum is 1)
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
	}

	return ok
}

func (rf *Raft) HeartBeat() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	leaderId := rf.me
	commitIndex := rf.commitIndex
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				rf.mu.Lock()

				// Construct proper AppendEntriesArgs
				prevLogIndex := rf.nextIndex[peer] - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
					prevLogTerm = rf.log[prevLogIndex].Term
				}

				entries := make([]LogEntry, 0) // heartbeat: no new entries

				args := &AppendEntriesArgs{currentTerm, leaderId, prevLogIndex, prevLogTerm, entries, commitIndex}

				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(peer, args, reply)
			}(i)
		}
	}
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	entry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, entry)

	index := rf.getLastLogIndex()
	term := rf.currentTerm

	for i := range rf.peers {
		if i != rf.me {
			go rf.replicateToPeer(i)
		}
	}

	return index, term, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.currentTerm += 1
	currentTerm := rf.currentTerm
	candidateId := rf.me
	rf.state = Candidate
	rf.lastHeartBeat = time.Now()
	rf.mu.Unlock()
	vote := 1
	var mu sync.Mutex
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int) {
				args := &RequestVoteArgs{}
				reply := &RequestVoteReply{}
				args.Term = currentTerm
				args.CandidateId = candidateId
				rf.sendRequestVote(peer, args, reply)
				if reply.Term > currentTerm {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
					}
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted == true {
					mu.Lock()
					vote += 1
					if vote > (len(rf.peers) / 2) {
						rf.mu.Lock()
						if (rf.state == Candidate) && (rf.currentTerm == currentTerm) {
							rf.state = Leader
						}
						rf.mu.Unlock()
					}
					mu.Unlock()
				}
			}(i)
		}
	}

}

// the service or tester wants to *create a Raft server*. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.lastHeartBeat = time.Now()
	// Actions to run the server (Follower/Candidate/Leader)
	go func() {
		for {
			rf.mu.Lock()
			last := rf.lastHeartBeat
			state := rf.state
			rf.mu.Unlock()
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

	go func() {
		for {
			rf.mu.Lock()
			commitIndex := rf.commitIndex
			lastApplied := rf.lastApplied
			rf.mu.Unlock()

			if commitIndex > lastApplied {
				entriesToApply := rf.log[lastApplied+1 : commitIndex+1]
				for _, entry := range entriesToApply {
					rf.mu.Lock()
					rf.lastApplied++
					appliedIndex := rf.lastApplied
					rf.mu.Unlock()
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      entry.Command,
						CommandIndex: appliedIndex,
					}

				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	return rf
}

func (rf *Raft) replicateToPeer(peer int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		prevLogIndex := rf.nextIndex[peer] - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
			prevLogTerm = rf.log[prevLogIndex].Term
		}
		entries := make([]LogEntry, len(rf.log[rf.nextIndex[peer]:]))
		copy(entries, rf.log[rf.nextIndex[peer]:])
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			if reply.Success {
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				rf.mu.Unlock()
				return
			} else {
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer]--
				}
			}
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

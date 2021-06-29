package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logEntries entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logEntries, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
"6.824/labgob"
"bytes"
"sync"
"sync/atomic"
"time"

//	"6.824/labgob"
"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive logEntries entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logEntries entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FollowerState = iota
	CandidateState
	LeaderState
)

const heartBeatTimeout = 150

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyMsgCond *sync.Cond



	myState      int
	//所有server的持久化状态
	currentTerm int
	votedFor    int
	logEntries  []LogEntry
	startLogs   []LogEntry
	// 所有server上的不稳定的状态
	commitIndex int
	lastApplied int
	// leader的可变状态
	nextIndex  []int
	matchIndex []int

	// lab 2D
	lastIncludedIndex int
	lastIncludedTerm int
	snapshot []byte

	hearBeatReset time.Time
	timerReset time.Time
	//timerReset      bool
	votedCount      int

	applyCh  chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock()
	defer rf.unLock()
	var term int
	var isleader bool

	term = rf.currentTerm

	if rf.myState == LeaderState {
		isleader = true
	} else {
		isleader = false
	}
	//DPrintf("2A   leader%d的isleader为%v", rf.me, isleader)
	// Your code here (2A).
	return term, isleader
}



//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(state []byte) {
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	d.Decode(&currentTerm)
	d.Decode(&votedFor)
	d.Decode(&logEntries)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)


	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logEntries = logEntries
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	//if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil {
	//	log.Panic("decode error")
	//} else {
	//}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.lock()
	defer rf.unLock()
	if rf.lastIncludedIndex >= lastIncludedIndex {
		DPrintf("raft[%d] receive condInstallSnapshot request, raft[%d]'s lastIncludedIndex[%d] 》= lastIncludedIndex[%d]", rf.me, rf.me, rf.lastIncludedIndex, lastIncludedIndex)
		return false
	} else {
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex   // 将commitIndex和lastApplied变为lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		DPrintf("raft[%d] receive condInstallSnapshot request, update rf.lastIncludedTerm[%d], rf.lastIncludedIndex[%d]", rf.me, rf.lastIncludedTerm, lastIncludedIndex)
		rf.SaveStateAndSnapshot(snapshot)
		return true
	}
	// Your code here (2D).
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the logEntries through (and including)
// that index. Raft should now trim its logEntries as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.lock()
	defer rf.unLock()
	if rf.lastIncludedIndex >= index {
		DPrintf("raft[%d] receive snapshot request, raft[%d]'s lastIncludedIndex[%d] 》= index[%d]", rf.me, rf.me, rf.lastIncludedIndex, index)
		return
	} else {
		ok, i, logEntry := rf.getLogEntry(index)
		var logEntries []LogEntry
		if ok {
			logEntries = rf.logEntries[i:]
		}

		rf.logEntries = logEntries
		rf.lastIncludedIndex = index
		rf.lastIncludedTerm = logEntry.Term
		rf.SaveStateAndSnapshot(snapshot)
		DPrintf("raft[%d] receive snapshot request, update lastIncludedIndex[%d], lastIncludedTerm[%d], logEntries[%v]", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.logEntries)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func(rf *Raft) SaveStateAndSnapshot(snapshot []byte) {
	var state []byte
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	state = w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		//2A
		//timerReset:  false,
		currentTerm: 0,
		votedFor:    -1,
		myState:     FollowerState,
		//2B
		commitIndex: 0,
		lastApplied: 0, // 应用于state machine的log entry最高index
		logEntries:  make([]LogEntry, 0),
		lastIncludedIndex: 0,
		lastIncludedTerm: 0,
	}

	rf.applyMsgCond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.lastIncludedIndex
	rf.applyCh = applyCh
	// start ticker goroutine to start elections
	go rf.ticker()
	// 需要实现一个单独的长时间运行的goroutine，在applyCh上按顺序发送已提交的日志条目，推进commitIndex的代码需要推动apply 的goroutine
	// 最简单的方式是使用条件变量（sync.Cond）

	go rf.sendCommandApplyMsg()
	// apply logEntries[lastApplied] to state machine

	return rf
}


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
	//	"bytes"
	"log"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
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

	logMu        sync.Mutex
	applyMsgCond *sync.Cond
	myState      int
	//所有server的持久化状态
	currentTerm int
	votedFor    int
	log         []logEntry

	// 所有server上的不稳定的状态
	commitIndex int
	lastApplied int
	// leader的可变状态
	nextIndex  []int
	matchIndex []int

	electionTimeout int
	timerReset      bool
	votedCount      int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	startEntry logEntry
}

type logEntry struct {
	command interface{}
	term    int
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

func (rf *Raft) updateCommitIndex() {
	lenLog:=len(rf.log)
	for i := rf.commitIndex+1; i < lenLog; i++ {
		count := 0
		for _, v := range rf.matchIndex {
			if v >= i {
				count++
			}
		}
		if count > lenLog/2 && rf.log[i].term == rf.currentTerm {
			rf.commitIndex = i
			return
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("2A   leader%d向节点%d发送appendEntries请求，请求在%d处添加log\n", rf.me, server, rf.nextIndex[server])
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("2A  请求%v", ok)
	rf.lock()
	DPrintf("2A  节点%d向节点%d发送心跳%v", rf.me, server, reply.Success)
	if ok {
		if reply.Success {
			rf.nextIndex[server] = len(rf.log)
			rf.matchIndex[server] += 1
			rf.unLock()
		} else {
			for args.Term >= reply.Term && !reply.Success {
				rf.nextIndex[server] -= 1
				args.Entries = rf.log[rf.nextIndex[server]:]
				rf.unLock()
				ok = rf.sendAppendEntries(server, args, reply)
			}
		}
	} else {
		rf.unLock()
	}

	return ok
}

func (rf *Raft) sendAppendEntriesToFollower() {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		Entries:      []logEntry{},
		LeaderCommit: rf.commitIndex,
	}

	for k, fNextIndex := range rf.nextIndex {
		if k == rf.me {
			continue
		}
		if len(rf.log)-1 >= fNextIndex {
			args.Entries = rf.log[fNextIndex:]
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(k, &args, &reply)
		}
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PervLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock()
	defer rf.unLock()
	DPrintf("2A  Candidate节点%d的Term%d和Follower节点%d的Term%d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	DPrintf("2A  节点%d向节点%d请求投票\n", args.CandidateId, rf.me)
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("2A   节点%d>Candidate的term，状态为%d, term为%d", rf.me, rf.myState, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.myState = FollowerState
		DPrintf("2A   节点%d变为Follower，状态为%d, term为%d, votedFor为%d", rf.me, rf.myState, rf.currentTerm, rf.votedFor)
	} else {
		DPrintf("2A   两个节点term相等\n")
	}
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		// candidate's log is at least as up-to-date as receiver's log, grant vote
		if args.LastLogIndex >= len(rf.log)-1 {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.timerReset = true
			DPrintf("2A   Candidate节点%d获得Follower节点%d的投票\n", rf.votedFor, rf.me)
		} else {
			reply.VoteGranted = false
		}
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// leader 向 follower发送appendEntry的请求，args为leader
	//fmt.Printf("Leader  %d  向Follower  %d  发送log entry数据\n", args.LeaderId, rf.me)
	rf.lock()
	defer rf.unLock()
	//DPrintf("2A   Leader%d的term为%d\n", args.LeaderId, args.Term)

	reply.Term = rf.currentTerm
	logLen := len(rf.log)
	// 不包含在pervLogIndex matches pervLogTerm
	if args.PrevLogIndex > logLen-1 || rf.log[args.PrevLogIndex].term != args.PervLogTerm {
		reply.Success = false
	}
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	}
	// entry和新的entry存在冲突，则删除entry和之后的所有条目
	for k, v := range args.Entries {
		index := args.PrevLogIndex + k
		if index < logLen && rf.log[index].term != v.term {
			rf.log = rf.log[:index]
		}
	}
	// append new entries not already in the log
	rf.log = append(rf.log, args.Entries...)

	// if leaderCommit > commitIndex, set CommitIndex = min(leaderCommit, index of last new entry
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
	}
	if args.Term >= rf.currentTerm {
		//DPrintf("2A   将节点%d变为follower\n", rf.me)
		rf.currentTerm = args.Term
		rf.myState = FollowerState
		reply.Success = true
		rf.timerReset = true
	}
	//fmt.Printf("Leader  %d  和Follower  %d  的term一样，它们的term为%d\n", args.LeaderId, rf.me, args.Term)

}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 不保证该命令提交到Raft的log中，因为leader可能会失败或输掉选举。
	if rf.killed() {
		return -1, -1, false
	}
	isLeader := rf.myState == LeaderState
	if !isLeader {
		return -1, -1, false
	}
	term := rf.currentTerm
	index := len(rf.log)
	rf.startEntry = logEntry{
		command: command,
		term:    term,
	}
	// Your code here (2B).

	return index, term, isLeader
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

func (log *logEntry) Apply() {
	DPrintf("运行了命令%v，它的term为%d\n", log.command, log.term)
}

func (rf *Raft) sendApplyMsg(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.logMu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyMsgCond.Wait()
		}
		rf.lastApplied++
		rf.log[rf.lastApplied].Apply()
		rf.logMu.Unlock()
	}
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
		timerReset:  false,
		currentTerm: 0,
		votedFor:    -1,
		myState:     FollowerState,
		//2B
		commitIndex: 0,
		lastApplied: 0, // 应用于state machine的log entry最高index
		log:         make([]logEntry, 1),
	}

	rf.log[0] = logEntry{}
	rf.applyMsgCond = sync.NewCond(&rf.logMu)
	for _, v := range peers {
		DPrintf("%d\n", v)
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// 需要实现一个单独的长时间运行的goroutine，在applyCh上按顺序发送已提交的日志条目，推进commitIndex的代码需要推动apply 的goroutine
	// 最简单的方式是使用条件变量（sync.Cond）
	go rf.sendApplyMsg(applyCh)
	// apply log[lastApplied] to state machine

	return rf
}

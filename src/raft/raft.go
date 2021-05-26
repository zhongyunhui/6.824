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
	"math/rand"
	"time"

	//	"bytes"
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

	myState int
	//所有server的持久化状态
	currentTerm int
	votedFor int
	log []logEntry

	// 所有server上的不稳定的状态
	commitIndex int
	lastApplied int
	// leader的不稳定状态
	nextIndex int
	matchIndex int


	electionTimeout int
	timerReset bool
	votedCount int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type logEntry struct {
	command interface{}
	term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool

	term = rf.currentTerm

	if rf.myState == LeaderState {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
	return term, isleader
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
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	// Your data here (2A).
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int

	PervLogTerm  int
	Entries      []byte
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}
//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("节点%d向节点%d请求投票\n", args.CandidateId, rf.me)
	// 判断发出选票的节点的term是否大于自身节点的term
	if args.Term < rf.currentTerm {
		DPrintf("Candidate节点%d的Term%d<Follower节点%d的Term%d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		DPrintf("Candidate节点%d的Term%d>Follower节点%d的Term%d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.myState = FollowerState
		rf.votedFor = args.CandidateId
	} else {
		DPrintf("两个节点term相等\n")
	}
	DPrintf("Candidate节点%d的Term%d>Follower节点%d的Term%d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		DPrintf("Candidate节点%d获得Follower节点%d的投票\n", args.CandidateId, rf.me)
		// 之后再做
		//if args.LastLogIndex >= rf.commitIndex {
		//	reply.VoteGranted = true
		//} else {
		//	reply.VoteGranted = false
		//}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		return
	}
	return
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// leader 向 follower发送appendEntry的请求，args为leader
	//fmt.Printf("Leader  %d  向Follower  %d  发送log entry数据\n", args.LeaderId, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.myState = FollowerState
		reply.Success = true
		rf.timerReset = true
		return
	}
	rf.timerReset = true
	DPrintf("Follower%d收到AppendEntry请求, timerReset为%v\n", rf.me, rf.timerReset)
	//fmt.Printf("Leader  %d  和Follower  %d  的term一样，它们的term为%d\n", args.LeaderId, rf.me, args.Term)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.mu.Lock()
	if rf.myState != CandidateState {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.myState = FollowerState
	}
	if reply.VoteGranted {
		rf.votedCount += 1
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.myState = FollowerState
	}
	return ok
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
	index := -1
	term := -1
	isLeader := true

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
//
// 检查选举时间是否复位，如果在选举时间过了之前完成，则返回true
// 如果在选举时间过了之后完成，则返回false
func (rf *Raft) checkTimerReset(state int, fn func()) bool {
	now := time.Now()
	rf.timerReset = false
	checkSum := float32(100)
	checkTime := float32(rf.electionTimeout) / checkSum

	for i:= 0; i < 100; i++ {
		rf.mu.Lock()
		if rf.myState != state {
			rf.timerReset = true
			rf.mu.Unlock()
			return true
		}
		rf.mu.Unlock()
		fn()
		rf.mu.Lock()
		if rf.timerReset {
			rf.mu.Unlock()
			return true
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(checkTime*1000)*time.Microsecond)
	}
	return false
}

func (rf *Raft) checkFollower() {
	for {
		//fmt.Println("该节点为Follower")
		rf.mu.Lock()
		//fmt.Printf("节点%dcheckFollower获得锁\n", rf.me)
		if rf.myState != FollowerState {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// 当election timer复位，则完成此次检查
		if rf.checkTimerReset(FollowerState, func(){}) {
			return
		}

		// 当election timer没有复位，而且rf没有投票的话，则将该节点变成Candidate
		rf.mu.Lock()
		//fmt.Printf("节点%d的checkFollower中的判断votedFor获得锁\n", rf.me)
		if rf.votedFor == -1 || !rf.timerReset{
			rf.votedFor = -1
			rf.myState = CandidateState
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !(rf.myState == CandidateState) {
		return
	}
	peersLen := len(rf.peers)
	if rf.votedCount > peersLen/2 {
		rf.myState = LeaderState
		rf.timerReset = true
		return
	}
}

func (rf *Raft) sendRequestVotes(args RequestVoteArgs) {
	for k, _ := range rf.peers {
		if k == rf.me {
			continue
		}
		reply := RequestVoteReply{}

		go rf.sendRequestVote(k, &args, &reply)
		// voteGranted 使用
	}
}

func (rf *Raft) startElection() {
	for {
		rf.mu.Lock()
		if rf.myState != CandidateState {
			return
		}
		rf.timerReset = false
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.votedCount = 1
		rf.mu.Unlock()
		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}
		rf.sendRequestVotes(args)

		if rf.checkTimerReset(CandidateState, rf.becomeLeader) {
			return
		}
	}
}

func (rf *Raft) sendEntries() {
	for {
		rf.mu.Lock()
		if rf.myState != LeaderState {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
		}
		rf.mu.Unlock()
		for k, _ := range rf.peers {
			if k == rf.me {
				continue
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(k, &args, &reply)
		}
		time.Sleep(time.Duration(heartBeatTimeout) * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	DPrintf("ticker获得锁\n")
	for rf.killed() == false {
		rf.mu.Lock()
		switch rf.myState {
		case FollowerState:
			rf.mu.Unlock()
			//fmt.Printf("followerState的case获得锁，节点%d为Follower，进入checkFollower中\n", rf.me)
			rf.checkFollower()
			break
		case CandidateState:
			rf.mu.Unlock()
			rf.startElection()
			break
		case LeaderState:
			// 发送心跳
			rf.mu.Unlock()
			rf.sendEntries()
			break
		default:
			panic("不合法的state")
		}
	}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
}

func RandInt(min, max int) int {
	if min >= max || min == 0 || max == 0 {
		return max
	}
	return rand.Intn(max-min) + min
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.timerReset = false
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.myState = FollowerState
	rf.electionTimeout = RandInt(400+150/len(peers)*rf.me, 450+150/len(peers)*rf.me)
	for _, v := range peers {
		DPrintf("%d\n",v)
	}


	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

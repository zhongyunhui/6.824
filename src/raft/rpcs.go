package raft

import "time"

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

	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ReplyLogIndex int
	ReplyLogTerm int
}


type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock()
	defer rf.unLock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || rf.myState == CandidateState{
		rf.currentTerm = args.Term
		rf.myState = FollowerState
	}

	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		DPrintf("raft[%d] refuse installSnapshot, raft[%d]'s lastIncludedIndex[%d] >= args.LastIncludedIndex[%d]", rf.me, rf.me, rf.lastIncludedIndex, args.LastIncludedIndex)
		return
	}
	rf.timerReset = time.Now()
	ok, logIndex, logEntry := rf.getLogEntry(args.LastIncludedIndex)
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	if ok && logIndex < len(rf.logEntries) {
		if logEntry.Term == args.LastIncludedTerm {
			rf.logEntries = rf.logEntries[logIndex:]
			DPrintf("raft[%d] receive installSnapshot RPC, update logEntries to [%d]", rf.me, rf.logEntries)
			rf.SaveStateAndSnapshot(args.Data)
			return
		}
	}
	rf.logEntries = make([]LogEntry, 0)
	DPrintf("raft[%d] receive installSnapshot RPC, update logEntries to [%d]", rf.me, rf.logEntries)
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	go func() {
		rf.applyCh <- applyMsg
	}()
}



//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock()
	defer rf.unLock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.myState = FollowerState
		rf.persist()
	}

	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		// candidate's logEntries is at least as up-to-date as receiver's logEntries, grant vote
		lastLogTerm := -1
		if len(rf.logEntries) != 0 {
			lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
		} else {
			lastLogTerm = rf.lastIncludedTerm
		}
		if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < rf.lastIncludedIndex+len(rf.logEntries)) {
			return
		} else {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.timerReset = time.Now()
			rf.persist()
			return
		}
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// leader 向 follower发送appendEntry的请求，args为leader
	//DPrintf("args为%d\n", args)
	//DPrintf("follower的logs为%v", rf.logEntries)
	rf.lock()
	defer rf.unLock()
	reply.ReplyLogIndex = -1
	reply.ReplyLogTerm = -1
	//DPrintf("2A   Leader%d的term为%d\n", args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.myState = FollowerState
		rf.persist()
	}

	if rf.myState == CandidateState {
		rf.myState = FollowerState
		rf.persist()
	}

	reply.Success = true
	rf.timerReset = time.Now()
	// 不包含在pervLogIndex matches pervLogTerm
	if args.PrevLogIndex > rf.lastIncludedIndex+len(rf.logEntries) {
		reply.ReplyLogIndex = rf.lastIncludedIndex+len(rf.logEntries)
		reply.Success = false
		return
	}

	_, _, logEntry := rf.getLogEntry(args.PrevLogIndex)
	if args.PrevLogIndex > rf.lastIncludedIndex && logEntry.Term != args.PrevLogTerm {
		reply.ReplyLogTerm = logEntry.Term
		for i := 1; i <= args.PrevLogIndex; i++ {
			_, _, log := rf.getLogEntry(i)
			if log.Term == reply.ReplyLogTerm {
				reply.ReplyLogIndex = i
				break
			}
		}
		reply.Success = false
		return
	}
	DPrintf("follower[%d]received the AppendEntries RPC from leader[%d], args.Entries[%v],args.LeaderCommit[%d], follower's logEntries[%v]", rf.me, args.LeaderId, args.Entries,args.LeaderCommit, rf.logEntries)
	// entry和新的entry存在冲突，则删除entry和之后的所有条目
	for k, v := range args.Entries {
		index := args.PrevLogIndex + 1 + k
		if index <= rf.lastIncludedIndex {
			continue
		}
		if index > rf.lastIncludedIndex + len(rf.logEntries) {
			rf.logEntries = append(rf.logEntries, v)
		} else {
			_, i, entry := rf.getLogEntry(index)
			if entry.Term != v.Term {
				rf.logEntries = rf.logEntries[:i-1]
				rf.logEntries = append(rf.logEntries, v)
			}
		}
	}
	rf.persist()
	terms := make([]int, len(rf.logEntries))
	for k, v := range rf.logEntries {
		terms[k] = v.Term
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.lastIncludedIndex + len(rf.logEntries))
		rf.applyMsgCond.Signal()
	}
}
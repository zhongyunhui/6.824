package raft

import (
	"log"
	"sort"
	"time"
)

func (rf *Raft) leaderInitialize() {
	rf.myState = LeaderState
	log.Printf("Candidate[%d] become to leader", rf.me)
	// reinitialized after election，初始化nextIndex和matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i:= 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.lastIncludedIndex+len(rf.logEntries)+1
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
	log.Printf("leader[%d] initialize finish，nextIndex[%d]", rf.me, rf.nextIndex[0])
}

func (rf *Raft) updateCommitIndex() {
	matchIndexs := make([]int, len(rf.matchIndex))
	copy(matchIndexs, rf.matchIndex)
	sort.Ints(matchIndexs)

	commitIndex := matchIndexs[len(rf.peers)/2]
	_, _, logEntry := rf.getLogEntry(commitIndex)
	if commitIndex > rf.commitIndex && logEntry.Term == rf.currentTerm {
		rf.commitIndex = commitIndex
		rf.applyMsgCond.Signal()
		DPrintf("leader[%d]update commitIndex to[%d]", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.currentTerm != args.Term {
		return
	}
	DPrintf("leader[%d]sendInstallSnapshot to raft[%d], args.Term[%v]", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	func(){
		rf.lock()
		defer rf.unLock()
		DPrintf("leader[%d]sendInstallSnapshot to raft[%d],[%v]", rf.me, server, ok)
		if ok {
			if args.Term != rf.currentTerm || rf.myState != LeaderState {
				return
			}
			if reply.Term > rf.currentTerm {
				DPrintf("leader[%d] handle sendInstallSnapshot to raft[%d], leader term[%d]<server term[%d]，become followerState", rf.me,server, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.myState = FollowerState
				rf.votedFor = -1
				rf.persist()
			} else {
				rf.nextIndex[server] = args.LastIncludedIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		}
	}()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.lock()
	if rf.currentTerm != args.Term || rf.myState != LeaderState{
		rf.unLock()
		return
	}
	DPrintf("leader[%d]sendAppendEntries to raft[%d]，args.Entries[%v], args.LeaderCommit[%v]", rf.me, server, args.Entries, args.LeaderCommit)
	if server == rf.me {
		// bug所在
		rf.nextIndex[server] = rf.lastIncludedIndex + len(rf.logEntries)+1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.updateCommitIndex()
		rf.unLock()
		return
	}
	rf.unLock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	func() {
		rf.lock()
		defer rf.unLock()
		if ok {
			if rf.currentTerm != args.Term || args.PrevLogIndex != rf.nextIndex[server]-1 {
				DPrintf("leader[%d]handle sendAppendEntries to raft[%d], this RPC is overdue, rf.currentTerm[%d] != args.Term[%d] || args.PrevLogIndex[%d] != rf.nextIndex[server][%d]-1", rf.me, server, rf.currentTerm, args.Term, args.PrevLogIndex, rf.nextIndex[server])
				return
			}
			if reply.Success {
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				DPrintf("leader[%d]handle sendAppendEntries to raft[%d], update nextIndex[%d] and matchIndex[%d]", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
				rf.updateCommitIndex()
			} else {
				if args.Term < reply.Term {
					DPrintf("leader[%d]handle sendAppendEntries to raft[%d], leader.currentTerm[%d] < reply.Term[%d]", rf.me, server, rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
					rf.myState = FollowerState
					rf.votedFor = -1
					rf.persist()
				} else {
					if reply.ReplyLogTerm != -1 {
						nextIndex := -1
						for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
							_, _, logEntry := rf.getLogEntry(i)
							if logEntry.Term == reply.ReplyLogTerm {
								nextIndex = i
								break
							}
						}
						if nextIndex != -1 {
							rf.nextIndex[server] = nextIndex + 1
						} else {
							rf.nextIndex[server] = reply.ReplyLogIndex
						}
					} else {
						rf.nextIndex[server] = reply.ReplyLogIndex + 1
					}
					DPrintf("leader[%d]handle sendAppendEntries to raft[%d], decrease nextIndex to [%d]", rf.me, server, rf.nextIndex[server])
				}
			}
		}
	}()
}

func (rf *Raft) sendAppendEntriesToFollower(leaderTerm int) {
	rf.lock()
	if leaderTerm != rf.currentTerm {
		rf.unLock()
		return
	}
	term := leaderTerm
	leaderCommit := rf.commitIndex
	lastIncludedIndex := rf.lastIncludedIndex
	lastIncludedTerm := rf.lastIncludedTerm
	DPrintf("leader[%d] start sendAppendEntries to followers，term[%d]", rf.me, term)

	for k, fNextIndex := range rf.nextIndex {
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			LeaderCommit: leaderCommit,
			PrevLogIndex: fNextIndex-1, // 紧挨着新entry的logIndex
		}

		if args.PrevLogIndex > rf.lastIncludedIndex {
			ok, _, logEntry := rf.getLogEntry(args.PrevLogIndex)
			if ok {
				args.PrevLogTerm = logEntry.Term
			}
		} else if args.PrevLogIndex == rf.lastIncludedIndex {
			args.PrevLogTerm = rf.lastIncludedTerm
		} else {
			snapshotArgs := InstallSnapshotArgs{
				Term:              term,
				LeaderId:          rf.me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			reply:=InstallSnapshotReply{}
			rf.unLock()
			go rf.sendInstallSnapshot(k, &snapshotArgs, &reply)
			rf.lock()
			continue
		}
		if rf.lastIncludedIndex+len(rf.logEntries) >= fNextIndex {
			args.Entries = rf.logEntries[fNextIndex-rf.lastIncludedIndex-1:]
		}
		reply := AppendEntriesReply{}
		rf.unLock()
		go rf.sendAppendEntries(k, &args, &reply)
		rf.lock()
	}
	rf.unLock()
}

func(rf *Raft) getLeaderTerm() int {
	rf.lock()
	defer rf.unLock()
	return rf.currentTerm
}

func (rf *Raft) LeaderLoop() {
	for !rf.killed(){
		leaderTerm := rf.getLeaderTerm()
		if !rf.checkState(LeaderState) {
			return
		}
		time.Sleep(heartBeatTimeout * time.Millisecond)
		rf.sendAppendEntriesToFollower(leaderTerm)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logEntries. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logEntries, since the leader
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
	rf.lock()

	isLeader := rf.myState == LeaderState
	//DPrintf("节点%d为leader%v", rf.me, rf.myState == LeaderState)
	if !isLeader {
		rf.unLock()
		return -1, -1, false
	}



	term := rf.currentTerm
	index := rf.lastIncludedIndex+len(rf.logEntries)+1
	rf.logEntries = append(rf.logEntries, LogEntry{
		Command:   command,
		Term:      rf.currentTerm,
	})
	rf.persist()
	DPrintf("command[%v]commit to leader[%d]'s log，term[%d]，index[%d]", command, rf.me, rf.currentTerm, index)

	rf.unLock()
	if rf.checkState(LeaderState) {
		rf.sendAppendEntriesToFollower(term)
	}


	return index, term, isLeader
}
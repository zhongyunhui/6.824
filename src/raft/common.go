package raft

import (
	"time"
)

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unLock() {
	rf.mu.Unlock()
}

func (log *LogEntry) Apply(index int) {

	//DPrintf("节点%d运行了命令%v，它的term为%d\n", index, logEntries.Command, logEntries.Term)
}

func (rf *Raft) checkCanApply() {
	rf.lock()
	if rf.commitIndex > rf.lastApplied {
		rf.unLock()
		rf.applyMsgCond.L.Lock()
		rf.applyMsgCond.Signal()
		rf.applyMsgCond.L.Unlock()
	} else {
		rf.unLock()
	}
}

func (rf *Raft) sendApplyMsg(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.lock()
		DPrintf("节点%d向客户端发送消息，它的commitIndex为%d,lastApplied为%d", rf.me, rf.commitIndex, rf.lastApplied)
		if rf.commitIndex <= rf.lastApplied {
			rf.unLock()
			rf.applyMsgCond.L.Lock()
			rf.applyMsgCond.Wait()
			rf.applyMsgCond.L.Unlock()
		} else {
			rf.unLock()
		}
		rf.lock()
		rf.logEntries[rf.lastApplied].Apply(rf.me)
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.logEntries[rf.lastApplied-1].Command,
			CommandIndex:  rf.lastApplied,
		}
		rf.unLock()
		DPrintf("RaftNode [%d] applyLog Command [%v] lastApplied和commandIndex[%d]", rf.me, applyMsg.Command, applyMsg.CommandIndex)
		applyCh <- applyMsg
	}
}



func (rf *Raft) checkState(state int) bool {
	rf.lock()
	defer rf.unLock()
	if rf.myState != state {
		DPrintf("2A  checkState中 节点%d的state从%d变成了%d", rf.me, state, rf.myState)
		rf.timerReset = true
	}
	if rf.timerReset {
		return true
	}
	return false
}

func (rf *Raft) checkTimerReset(state int) bool {
	now := time.Now()
	rf.electionTimeout = RandInt(400, 600)
	rf.lock()
	rf.timerReset = false
	me := rf.me
	rf.unLock()
	checkSum := float64(300)
	checkTime := float64(rf.electionTimeout) / checkSum

	for i := 0; i < 300; i++ {
		if rf.checkState(state) {
			return true
		}
		time.Sleep(time.Duration(checkTime*1000) * time.Microsecond)
	}
	DPrintf("2A   节点%d的timerout为%v", me, time.Since(now))
	if rf.checkState(state) {
		return true
	} else {
		return false
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.lock()
		DPrintf("2A   节点【%d】进入ticker循环", rf.me)
		switch rf.myState {
		case FollowerState:
			rf.unLock()
			rf.checkFollower()
			break
		case CandidateState:
			rf.unLock()
			rf.startElection()
			break
		case LeaderState:
			// 发送心跳
			rf.unLock()
			//DPrintf("2A   节点%d变成leader\n", rf.me)
			rf.sendAppendEntriesToFollower()
			break
		default:
			rf.unLock()
			panic("不合法的state")
		}
	}
	DPrintf("跳出循环")
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
}



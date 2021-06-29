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

func (rf *Raft) getTerm() int {
	rf.lock()
	defer rf.unLock()
	return rf.currentTerm
}

func (rf *Raft) sendCommandApplyMsg() {
	for !rf.killed() {
		applyMsgs := make([]ApplyMsg, 0)

		func() {
			rf.lock()
			defer rf.unLock()
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				DPrintf("raft[%d]send command to server, commitIndex为[%d],lastApplied为[%d]", rf.me, rf.commitIndex, rf.lastApplied)
				_, _,logEntry := rf.getLogEntry(rf.lastApplied)
				applyMsgs = append(applyMsgs, ApplyMsg{
					CommandValid:  true,
					Command:       logEntry.Command,
					CommandIndex:  rf.lastApplied,
				})
			}
		}()
		for _, v := range applyMsgs {
			rf.applyCh <- v
		}
		rf.lock()
		rf.applyMsgCond.Wait()
		rf.unLock()
	}
}

func (rf *Raft) getLogEntry(index int) (bool, int, LogEntry) {
	logIndex := index - rf.lastIncludedIndex
	if logIndex > len(rf.logEntries) || logIndex <= 0 {
		return false, logIndex, LogEntry{}
	}
	logEntry := rf.logEntries[logIndex-1]
	return true, logIndex, logEntry
}



func (rf *Raft) checkState(state int) bool {
	rf.lock()
	defer rf.unLock()
	if rf.myState != state {
		return false
	}
	return true
}



func (rf *Raft) initTimerReset() {
	rf.lock()
	defer rf.unLock()
	rf.timerReset = time.Now()
}

func (rf *Raft) getTimerReset() time.Time {
	rf.lock()
	defer rf.unLock()
	return rf.timerReset
}

func (rf *Raft) getMyState() int{
	rf.lock()
	defer rf.unLock()
	state := rf.myState
	return state
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		state := rf.getMyState()
		switch state {
		case FollowerState:
			rf.checkFollower()
			break
		case CandidateState:
			rf.startElection()
			break
		case LeaderState:
			// 发送心跳
			//DPrintf("2A   节点%d变成leader\n", rf.me)
			rf.LeaderLoop()
			break
		default:
			panic("不合法的state")
		}
	}
	DPrintf("跳出循环")
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
}



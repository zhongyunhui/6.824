package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unLock() {
	rf.mu.Unlock()
}

func (rf *Raft) sendApplyMsg(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		applyMsgs := make([]ApplyMsg, 0)


		func() {
			rf.lock()
			defer rf.unLock()
			for rf.commitIndex > rf.lastApplied {
				DPrintf("节点[%d]向客户端发送消息，它的commitIndex为[%d],lastApplied为[%d]", rf.me, rf.commitIndex, rf.lastApplied)
				rf.lastApplied++
				applyMsgs = append(applyMsgs, ApplyMsg{
					CommandValid:  true,
					Command:       rf.logEntries[rf.lastApplied-1].Command,
					CommandIndex:  rf.lastApplied,
				})
			}
		}()
		for _, v := range applyMsgs {
			applyCh <- v
		}
	}
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
	rf.TimerReset = time.Now()
}
func (rf *Raft) getTimerReset() time.Time {
	rf.lock()
	defer rf.unLock()
	return rf.TimerReset
}

func (rf *Raft) checkTimerReset(state int) bool {
	rf.initTimerReset()
	timeout := time.Duration(400+rand.Int31n(150))*time.Millisecond
	for time.Since(rf.getTimerReset()) <= timeout && rf.getMyState() == state {
		time.Sleep(time.Millisecond)
	}
	DPrintf("节点[%d]的timerout为[%v]", rf.me, time.Since(rf.getTimerReset()))

	//for i := 0; i < 300; i++ {
	//	if rf.getTimerReset() || rf.getMyState() != state{
	//		return true
	//	}
	//	time.Sleep(time.Duration(checkTime*1000) * time.Microsecond)
	//}
	return false
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
			rf.sendAppendEntriesToFollower()
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



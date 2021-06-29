package raft

import (
	"log"
	"math/rand"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	log.SetPrefix("raft-----")
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func RandInt(min, max int) int {
	if min >= max || min == 0 || max == 0 {
		return max
	}
	return rand.Intn(max-min) + min
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
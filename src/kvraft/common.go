package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GET = iota
	PUT
	APPEND
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	SeqId int64
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	SeqId int64
	ClientId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

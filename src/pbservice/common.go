package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Ans struct {
	Data string
	Time int64
}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Req#
	Reqn int64
	// Operation
	Op string
	// Forwarded?
	Forward bool
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	// Req#
	Reqn int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.

const ReqTTL = 30

type LoadDBArgs struct {
	Err  Err
	Data map[string]string
	Req  map[int64]Ans
}

type LoadDBReply struct {
	Err Err
}

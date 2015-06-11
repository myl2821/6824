package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	Me string
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

	Me string
	// Req#
	Reqn int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type LoadDBArgs struct {
	Err  Err
	Data map[string]string
}

type LoadDBReply struct {
	Err Err
}

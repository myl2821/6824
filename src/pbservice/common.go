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

	// Req#
	Reqn     uint
	// Operation
	Op    string
	// If this destrction is forwarded
	Forward  bool
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	// Req#
	Reqn     uint
	// If this destrction is forwarded
	Forward  bool
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.

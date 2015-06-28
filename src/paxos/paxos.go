package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "time"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	npeers   int
	inst     map[int]PaxosInstance
	done     []int
	doneLock sync.Mutex
}

// suppose we don't need to record n
// because we know what n is
type PaxosReply struct {
	Result bool
	Status Fate
	N_a    int64
	V_a    interface{}
}

type PaxosArg struct {
	Seq  int
	N    int64
	V    interface{}
	Done int
	Me   int
}

type PaxosInstance struct {
	n_p  int64
	n_a  int64
	v_a  interface{}
	fate Fate
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) Prepare(arg PaxosArg, reply *PaxosReply) error {
	px.mu.Lock()
	inst, exist := px.inst[arg.Seq]
	if exist {
		if inst.fate == Decided {
			reply.Result = false
			reply.Status = Decided
			reply.N_a = inst.n_a
			reply.V_a = inst.v_a
			px.mu.Unlock()
			return nil
		}
		if arg.N > inst.n_p {
			inst.n_p = arg.N
			px.inst[arg.Seq] = inst
			reply.Result = true
			reply.N_a = inst.n_a
			reply.V_a = inst.v_a
			reply.Status = inst.fate
		} else {
			// reject
			reply.Result = false
			reply.Status = inst.fate
		}
	} else {
		// new instance seen
		px.inst[arg.Seq] = PaxosInstance{arg.N, -1, nil, Pending}
		reply.Status = Pending
		reply.Result = true
		reply.N_a = -1
		reply.V_a = nil
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Accept(arg PaxosArg, reply *PaxosReply) error {
	px.mu.Lock()
	inst, exist := px.inst[arg.Seq]
	if exist {
		if inst.fate == Decided {
			reply.Result = false
			reply.Status = Decided
			reply.N_a = inst.n_a
			reply.V_a = inst.v_a
		}
		if arg.N >= inst.n_p {
			inst.n_p = arg.N
			inst.n_a = arg.N
			inst.v_a = arg.V
			px.inst[arg.Seq] = inst
			reply.Result = true
		} else {
			// reject
			reply.Result = false
			reply.Status = inst.fate
		}
	} else {
		// new instance seen
		px.inst[arg.Seq] = PaxosInstance{arg.N, arg.N, arg.V, Pending}
		reply.Result = true
		reply.Status = inst.fate
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Decided(arg PaxosArg, reply *PaxosReply) error {
	px.mu.Lock()
	inst, exist := px.inst[arg.Seq]
	px.done[arg.Me] = arg.Done
	if exist && inst.fate == Pending {
		px.inst[arg.Seq] = PaxosInstance{arg.N, arg.N, arg.V, Decided}
		px.done[arg.Me] = arg.Done
	} else {
		// new instance seen
		px.inst[arg.Seq] = PaxosInstance{arg.N, arg.N, arg.V, Decided}
	}
	px.UpdateDone()
	px.mu.Unlock()
	return nil
}

func (px *Paxos) SendPrepare(seq int, n int64, v interface{}) (bool, interface{}) {
	ch := make(chan PaxosReply, px.npeers)
	for i, pax := range px.peers {
		go func(pax string) {
			var reply PaxosReply
			arg := PaxosArg{Seq: seq, N: n, V: v}
			if i == px.me {
				_ = px.Prepare(arg, &reply)
			} else {
				ok := call(pax, "Paxos.Prepare", arg, &reply)
				if ok == false {
					// cannot connect to peer, regard it as reject
					reply.Result = false
					reply.Status = Pending
				}
			}
			ch <- reply
		}(pax)
	}

	var n_a int64 = -1
	var v_a interface{} = nil
	consensus := false
	nagree := 0

	for i := 0; i < px.npeers; i++ {
		reply := <-ch
		if reply.Result == true && consensus == false {
			nagree++
			if reply.N_a > n_a {
				n_a = reply.N_a
				v_a = reply.V_a
			}
			if nagree > px.npeers/2 {
				consensus = true
				if n_a == -1 {
					// no proposal seen from others
					v_a = v
				}
			}
		} else if reply.Result == false && reply.Status == Decided {
			arg := PaxosArg{seq, reply.N_a, reply.V_a, px.done[px.me], px.me}
			var _reply PaxosReply
			px.Decided(arg, &_reply)
			return false, nil
		}
	}
	return consensus, v_a
}

func (px *Paxos) UpdateDone() {
	px.doneLock.Lock()
	done := px.done[px.me] + 1
	for {
		inst, exist := px.inst[done]
		if exist {
			if inst.fate == Decided {
				px.done[px.me] = done
				done++
			} else {
				break
			}
		} else {
			break
		}
	}
	px.doneLock.Unlock()
}

func (px *Paxos) SendAccept(seq int, n int64, v interface{}) bool {
	ch := make(chan PaxosReply, px.npeers)
	for i, pax := range px.peers {
		go func(pax string) {
			var reply PaxosReply
			arg := PaxosArg{Seq: seq, N: n, V: v}
			if i == px.me {
				px.Accept(arg, &reply)
			} else {
				ok := call(pax, "Paxos.Accept", arg, &reply)
				if ok == false {
					// cannot connect to peer, regard it as reject
					reply.Result = false
				}
			}
			ch <- reply
		}(pax)
	}
	consensus := false
	nagree := 0
	for i := 0; i < px.npeers; i++ {
		reply := <-ch
		if reply.Result == true && consensus == false {
			nagree++
			if nagree > px.npeers/2 {
				consensus = true
			}
		} else if reply.Result == false && reply.Status == Decided {
			arg := PaxosArg{seq, reply.N_a, reply.V_a, px.done[px.me], px.me}
			var _reply PaxosReply
			px.Decided(arg, &_reply)
			return false
		}
	}

	return consensus
}

func (px *Paxos) SendDecided(seq int, v interface{}) {
	px.UpdateDone()
	for i, pax := range px.peers {
		go func(pax string) {
			var reply PaxosReply
			arg := PaxosArg{seq, -1, v, px.done[px.me], px.me}
			if i == px.me {
				px.Decided(arg, &reply)
			} else {
				_ = call(pax, "Paxos.Decided", arg, &reply)
			}
		}(pax)
	}
}

func (px *Paxos) Proposer(seq int, v interface{}) {
	for {
		if px.done[px.me] >= seq {
			break
		}
		// just use UNIX timestamp as proposal number
		n := time.Now().Unix()
		prepare_ok, v_a := px.SendPrepare(seq, n, v)
		if prepare_ok {
			accept_ok := px.SendAccept(seq, n, v_a)
			if accept_ok {
				px.SendDecided(seq, v_a)
				break
			}
		}
		//XXX: should raise fail after some times
		time.Sleep(10 * time.Millisecond)
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	go func() {
		if seq >= px.Min() {
			px.Proposer(seq, v)
		}
	}()
}

func (px *Paxos) DoneAcceptor(arg PaxosArg, reply *PaxosReply) error {
	px.doneLock.Lock()
	px.done[arg.Me] = arg.Done
	px.doneLock.Unlock()
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	var reply PaxosReply
	arg := PaxosArg{seq, -1, nil, seq, px.me}
	for i, pax := range px.peers {
		if i != px.me {
			_ = call(pax, "Paxos.DoneAcceptor", arg, &reply)
		} else {
			px.done[px.me] = seq
		}
	}
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	max := 0
	for k, _ := range px.inst {
		if k > max {
			max = k
		}
	}

	if px.done[px.me] > max {
		max = px.done[px.me]
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	min := px.done[px.me]
	for _, v := range px.done {
		if v < min {
			min = v
		}
	}

	// Paxos is required to have forgotten all information
	// about any instances it knows that are < Min().
	// The point is to free up memory in long-running
	// Paxos-based servers.
	for k, v := range px.inst {
		if k <= min && v.fate == Decided {
			delete(px.inst, k)
		}
	}

	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	inst, exist := px.inst[seq]
	px.mu.Unlock()
	if exist {
		return inst.fate, inst.v_a
	} else {
		if seq < px.Min() {
			return Forgotten, nil
		}
		return Pending, nil
	}
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.npeers = len(peers)
	px.inst = make(map[int]PaxosInstance)
	px.done = make([]int, px.npeers)
	for i, _ := range peers {
		px.done[i] = -1
	}
	// over.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

// Custom subroutines

package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.

	// K-V pair memcache
	data       map[string]string

	// latest View
	view       viewservice.View

	// I am primary?
	primary	   bool

	// Map client to its latest req#
	cli2req    map[string]int64

	// map req# to its answer
	req2ans    map[int64]string

}

// Forward instruction to backup
func (pb *PBServer) Forward(serv string, arg *PutAppendArgs, reply *PutAppendReply) error {
	arg.Forward = true
	ok := call(serv, "PBServer.PutAppend", arg, reply)
	for !ok {
		time.Sleep(viewservice.PingInterval)
		ok = call(serv, "PBServer.PutAppend", arg, reply)
	}
	if reply.Err != "" {
		// XXX being rejected
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	getAns := func(reqn int64) {
		pb.cli2req[args.Me] = reqn
		ans := pb.data[args.Key]
		log.Println(args.Key)
		pb.req2ans[reqn] = ans
		reply.Value = ans
	}

	// Don't forward Get option
	if pb.primary {
		reqn, exists := pb.cli2req[args.Me]
		if exists {
			if args.Reqn == reqn {
				// dup Operation
				reply.Value = pb.req2ans[reqn]
			} else {
				// delete old answer
				delete(pb.req2ans, reqn)
				getAns(reqn)
			}
		} else {
			// New Clerk
			getAns(reqn)
		}
	} else {
		log.Println("x")
		reply.Err = "Query backup"
	}

	// XXX Map new answer
	// XXX Unmap old answer

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.

	// XXX Reject forward when needed

	update := func(reqn int64) {
		pb.cli2req[args.Me] = reqn
		switch args.Op {
		case "Put":
			pb.data[args.Key] = args.Value
		case "Append":
			pb.data[args.Key] += args.Value
		}

		pb.req2ans[reqn] = ""
		if pb.primary && pb.view.Backup != "" {
			pb.Forward(pb.view.Backup, args ,reply)
		}
	}


	reqn, exists := pb.cli2req[args.Me]
	if exists {
		if args.Reqn == reqn {
			// dup Operation
			return nil
		} else {
			// delete old answer
			delete(pb.req2ans, reqn)
			update(reqn)
		}
	} else {
		// New Clerk
		update(reqn)
	}

	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	// learn latest view status
	res, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil {
		pb.view = res
		pb.primary = (pb.me == pb.view.Primary)
	} else {
		log.Fatal("vs conn abort!!!")
	}
	// XXX: manage transfer of state from primary to new backup.
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.data = make(map[string]string)
	pb.cli2req = make(map[string]int64)
	pb.req2ans = make(map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}

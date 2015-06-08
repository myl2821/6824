package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	volunteer  string // promoted to backup when needed
	view       View
	newView    View

	needupdate bool
	primaryAck bool

	pmiss	   int32 // missed ping of primary
	bmiss      int32 // missed ping of backup
}

func (vs *ViewServer) updateView (Viewnum uint, primary string, backup string) {
	if primary == backup {
		log.Fatal("Inner error: primary equals to backup")
	}
	vs.needupdate = true
	v := new(View)
	v.Viewnum = Viewnum
	v.Primary = primary
	v.Backup = backup
	vs.newView = *v
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	client := args.Me
	clientVn := args.Viewnum
	viewerVn := vs.view.Viewnum

	// Primary acked current view
	if client == vs.view.Primary && clientVn == vs.view.Viewnum {
		vs.primaryAck = true
	}

	// new worker?
	if clientVn == 0 {
		if viewerVn == 0 {
			vs.view.Viewnum = 1
			vs.view.Primary = client
		} else if vs.view.Backup == "" && client != vs.view.Primary && client != vs.volunteer {
			vs.updateView(viewerVn + 1, vs.view.Primary, client)
		} else if vs.volunteer == "" && client != vs.view.Backup && client != vs.view.Primary {
			vs.volunteer = client
		}
	}

	// refresh P/B state
	// Just let it die if ViewNum is not correct
	if vs.view.Primary == client && clientVn == vs.view.Viewnum {
		vs.pmiss = 0
	}
	if vs.view.Backup == client && clientVn == vs.view.Viewnum {
		vs.bmiss = 0
	}
	reply.View = vs.view
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.view
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	if vs.view.Primary != "" {
		vs.pmiss++
		if vs.pmiss >= DeadPings {
			if vs.view.Backup != "" {
				if vs.volunteer != "" {
					vs.updateView(vs.view.Viewnum+1, vs.view.Backup, vs.volunteer)
					vs.volunteer = ""
				} else {
					vs.updateView(vs.view.Viewnum+1, vs.view.Backup, "")
				}
			}
		}
	}

	if vs.view.Backup != "" {
		vs.bmiss++
		if vs.bmiss >= DeadPings {
			if vs.volunteer != "" {
				vs.updateView(vs.view.Viewnum+1, vs.view.Primary, vs.volunteer)
				vs.volunteer = ""
			} else {
				vs.updateView(vs.view.Viewnum+1, vs.view.Primary, "")
			}
		}
	}

	// proceed view update!
	if vs.primaryAck && vs.needupdate {
		vs.view = vs.newView
		vs.primaryAck = false
		vs.needupdate = false
		vs.pmiss = 0
		vs.bmiss = 0
	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	view := new(View)
	view.Viewnum = 0
	view.Primary = ""
	view.Backup = ""
	vs.view = *view

	vs.volunteer = ""
	vs.pmiss = 0
	vs.bmiss = 0

	vs.primaryAck = false
	vs.needupdate = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}

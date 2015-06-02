package mapreduce

import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	busy bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	log.Printf("Run Master")
	mr.Workers = make(map[string]*WorkerInfo)
	go func() {
		for {
			select {
			case worker := <-mr.registerChannel:
				log.Printf("Receive worker:%s\n", worker)
				_, ok := mr.Workers[worker]
				if !ok {
					// get rid of dup regist
					info := new(WorkerInfo)
					info.address = worker
					info.busy = false
					mr.Workers[worker] = info
					mr.idleChannel <- worker
				}
			case <-mr.stopRegistChannel:
				log.Printf("Master: Stop regist")
				return
			}
		}
	}()

	doJob := func (t JobType, jn int) {
		var nOtherPhase int
		var recver chan bool
		switch t {
		case Map:
			nOtherPhase = mr.nReduce
			recver = mr.mapChannel
		case Reduce:
			nOtherPhase = mr.nMap
			recver = mr.reduceChannel
		}
		args := &DoJobArgs{mr.file, t, jn, nOtherPhase}
		var reply DoJobReply
		for {
			w := <-mr.idleChannel
			mr.Workers[w].busy = true
			ok := call(w, "Worker.DoJob", args, &reply)
			if ok {
				recver <- true
				mr.Workers[w].busy = false
				mr.idleChannel <- w
				return
			}
		}
	}

	for i := 0; i < mr.nMap; i++ {
		go doJob(Map, i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mr.mapChannel
	}

	for i := 0; i < mr.nReduce; i++ {
		go doJob(Reduce, i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-mr.reduceChannel
	}

	for _, v := range mr.Workers {
		if !v.busy {
			<-mr.idleChannel
		}
	}

	mr.stopRegistChannel <- true
	return mr.KillWorkers()
}

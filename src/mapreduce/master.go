package mapreduce

import "container/list"
import "fmt"
import "log"

type WorkerInfo struct {
	address string
	// You can add definitions here.
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
					mr.Workers[worker] = info
					mr.idleChannel <- worker
				}
			case <-mr.stopRegistChannel:
				log.Printf("Master: Stop regist")
				return
			}
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		worker := <-mr.idleChannel
		go func(w string, jn int) {
			args := &DoJobArgs{mr.file, Map, jn, mr.nReduce}
			var reply DoJobReply
			ok := call(w, "Worker.DoJob", args, &reply)
			if !ok {
				log.Fatal("Map Failed")
				// XXX do rescue
			} else {
				mr.mapChannel <- true
			}
			mr.idleChannel <- w
		}(worker, i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mr.mapChannel
	}

	for i := 0; i < mr.nReduce; i++ {
		worker := <-mr.idleChannel
		go func(w string, jn int) {
			args := &DoJobArgs{mr.file, Reduce, jn, mr.nMap}
			var reply DoJobReply
			ok := call(w, "Worker.DoJob", args, &reply)
			if !ok {
				log.Fatal("Reduce Failed")
				// XXX do rescue
			} else {
				mr.reduceChannel <- true
			}
			mr.idleChannel <- w
		}(worker, i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-mr.reduceChannel
	}

	for i := 0; i < len(mr.Workers); i++ {
		<-mr.idleChannel
	}

	mr.stopRegistChannel <- true
	return mr.KillWorkers()
}

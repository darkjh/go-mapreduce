package mapreduce

import "container/list"
import "fmt"

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

func (mr *MapReduce) CallMap(worker string, jobNum int) {
	jobArgs := DoJobArgs{File: mr.file, Operation: Map,
		JobNumber: jobNum, NumOtherPhase: mr.nReduce}
	reply := new(DoJobReply)
	call(worker, "Worker.DoJob", jobArgs, &reply)
	mr.MapDoneChannel <- true

}

func (mr *MapReduce) CallReduce(worker string, jobNum int) {
	jobArgs := DoJobArgs{File: mr.file, Operation: Reduce,
		JobNumber: jobNum, NumOtherPhase: mr.nMap}
	reply := new(DoJobReply)
	call(worker, "Worker.DoJob", jobArgs, &reply)
	mr.ReduceDoneChannel <- true
}

func (mr *MapReduce) RunMaster() *list.List {
	// run maps
	for i := 0; i < mr.nMap; i++ {
		worker := <-mr.registerChannel
		go mr.CallMap(worker, i)
	}
	// wait for all maps to complet
	for i := 0; i < mr.nMap; i++ {
		<-mr.MapDoneChannel
	}
	fmt.Println("Maps finished ...")

	// run reduces
	for i := 0; i < mr.nReduce; i++ {
		worker := <-mr.registerChannel
		fmt.Println(worker)
		go mr.CallReduce(worker, i)
	}
	for i := 0; i < mr.nReduce; i++ {
		<-mr.ReduceDoneChannel
	}

	fmt.Println("Reduces finished ...")
	return mr.KillWorkers()
}

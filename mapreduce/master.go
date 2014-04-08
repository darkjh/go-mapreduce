package mapreduce

import "container/list"
import "net/rpc"
import "net"
import "fmt"
import "log"
import "os"
import "os/signal"

type WorkerInfo struct {
	address string
}

type Master struct {
	Address string // eg. `localhost:7777`
	// TODO these should be specific about a job, need JobContext
	registerChannel   chan string
	mapDoneChannel    chan bool
	reduceDoneChannel chan bool
	submitChannel     chan bool
	l                 net.Listener
	alive             bool
	stats             *list.List
	job               Job                    // job to execute
	Workers           map[string]*WorkerInfo // worker state
}

func (m *Master) Register(args *RegisterArgs, res *RegisterReply) error {
	DPrintf("Register: worker %s\n", args.Worker)
	worker := args.Worker
	m.registerChannel <- worker
	m.Workers[worker] = &WorkerInfo{address: worker}
	res.OK = true
	return nil
}

func (m *Master) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
	DPrintf("Shutdown: registration server\n")
	defer os.Exit(0)
	m.alive = false
	m.l.Close() // causes the Accept to fail
	m.KillWorkers()
	return nil
}

func (m *Master) SubmitJob(args *SubmitArgs, res *SubmitReply) error {
	DPrintf("Submit")
	m.job = args.Job
	res.OK = true
	m.submitChannel <- true
	return nil
}

func (m *Master) StartRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", m.Address)
	if e != nil {
		log.Fatal("RPCServer", m.Address, " error: ", e)
	}
	m.l = l

	// now that we are listening on the master address, can fork off
	// accepting connections to another thread.
	go func() {
		for m.alive {
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				DPrintf("RPCServer: accept error", err)
				break
			}
		}
		DPrintf("RPCServer: done\n")
	}()
}

func (m *Master) CleanupRegistration() {
	args := &ShutdownArgs{}
	var reply ShutdownReply
	ok := call(m.Address, "Master.Shutdown", args, &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", m.Address)
	}
	DPrintf("CleanupRegistration: done\n")
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each worker has performed.
func (m *Master) KillWorkers() *list.List {
	l := list.New()
	for _, w := range m.Workers {
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

func (m *Master) CallMap(worker string, jobNum int) {
	jobArgs := DoJobArgs{File: m.job.InputPath, Operation: Map,
		JobNumber: jobNum, NumOtherPhase: m.job.NReduce}
	reply := new(DoJobReply)
	call(worker, "Worker.DoJob", jobArgs, &reply)
	m.mapDoneChannel <- true
}

func (m *Master) CallReduce(worker string, jobNum int) {
	jobArgs := DoJobArgs{File: m.job.InputPath, Operation: Reduce,
		JobNumber: jobNum, NumOtherPhase: m.job.NMap}
	reply := new(DoJobReply)
	call(worker, "Worker.DoJob", jobArgs, &reply)
	m.reduceDoneChannel <- true
}

// TODO modify this
func (m *Master) RunJob() *list.List {
	// run maps
	nMap := m.job.NMap
	nReduce := m.job.NReduce
	for i := 0; i < nMap; i++ {
		worker := <-m.registerChannel
		go m.CallMap(worker, i)
	}
	// wait for all maps to complet
	for i := 0; i < nMap; i++ {
		<-m.mapDoneChannel
	}
	fmt.Println("Maps finished ...")

	// run reduces
	for i := 0; i < nReduce; i++ {
		worker := <-m.registerChannel
		fmt.Println(worker)
		go m.CallReduce(worker, i)
	}
	for i := 0; i < nReduce; i++ {
		<-m.reduceDoneChannel
	}

	fmt.Println("Reduces finished ...")
	return nil
}

func (m *Master) Run() {
	// wait for job
	fmt.Println("Waiting for job ...")
	<-m.submitChannel

	// start doing MR
	input := m.job.InputPath
	fmt.Printf("Run mapreduce job %s %s\n",
		m.Address, input)

	// prepare splits
	mr := InitMapReduce(m.job)
	mr.Split(input)
	m.RunJob()
	mr.Merge()

	// notify when job finishes
	fmt.Printf("%s: MapReduce done\n", m.Address)
}

func InitMaster(address string) *Master {
	m := new(Master)
	m.Address = address
	m.alive = true
	// TODO better way for this channeling
	m.registerChannel = make(chan string, 100)
	m.mapDoneChannel = make(chan bool, 100)
	m.reduceDoneChannel = make(chan bool, 100)
	m.submitChannel = make(chan bool)

	// init workers info
	m.Workers = make(map[string]*WorkerInfo)

	return m
}

// trap interrupt signal to do some cleanup then exit
func (m *Master) ListenOnExit() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			log.Printf("captured %v, exiting..", sig)
			// stop workers
			m.KillWorkers()
			os.Exit(1)
		}
	}()
}

// Init and run the master process
func RunMasterProcess(address string) {
	m := InitMaster(address)
	m.StartRPCServer()
	m.ListenOnExit()
	// long-running master process
	for true {
		m.Run()
	}
}

package mapreduce

import "fmt"

// issue a RPC call to the master for submitting a job
func SubmitJob(job Job, master string) {
	args := &SubmitArgs{Job: job}
	var reply SubmitReply
	ok := call(master, "Master.SubmitJob", args, &reply)
	if ok == false {
		fmt.Printf("SubmitJob: error when submit job to %s",
			master)
	}
}

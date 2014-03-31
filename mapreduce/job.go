package mapreduce

import "path"

type Job struct {
	NMap      int    // number of Map tasks
	NReduce   int    // number of Reduce tasks
	InputPath string // input path, eg `examples/test.txt`

	// TODO need to associate MapFunc and ReduceFunc in Job
}

func (job *Job) InputDir() string {
	return path.Dir(job.InputPath)
}

func (job *Job) InputFile() string {
	return path.Base(job.InputPath)
}

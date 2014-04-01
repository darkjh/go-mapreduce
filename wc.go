package main

import "os"
import "fmt"
import "github.com/darkjh/go-mapreduce/mapreduce"
import "container/list"
import "strings"
import "strconv"
import "unicode"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
	notLetter := func(r rune) bool {
		return !unicode.IsLetter(r)
	}
	fields := strings.FieldsFunc(value, notLetter)

	l := list.New()
	for _, f := range fields {
		kv := mapreduce.KeyValue{Key: f, Value: "1"}
		l.PushBack(kv)
	}
	return l
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	count := 0
	// iterate over list
	for e := values.Front(); e != nil; e = e.Next() {
		v := e.Value.(string)
		intValue, _ := strconv.Atoi(v)
		count += intValue
	}
	return strconv.Itoa(count)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go sequential x.txt)
// 2) Master (e.g., go run wc.go master localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778)
// 4) Submit (e.g., go run wc.go submit x.txt localhost:7777)
func main() {
	if len(os.Args) < 3 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
		return
	}

	switch os.Args[1] {
	case "sequential":
		job := mapreduce.Job{NMap: 5, NReduce: 3,
			InputPath: os.Args[2]}
		mapreduce.RunSingle(job, Map, Reduce)
	case "master":
		m := mapreduce.RunMaster(os.Args[2])
		// Wait until MR is done
		<-m.AllDoneChannel
	case "worker":
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, -1)
	case "submit":
		job := mapreduce.Job{NMap: 2, NReduce: 1,
			InputPath: os.Args[2]}
		mapreduce.SubmitJob(job, os.Args[3])
	}
}

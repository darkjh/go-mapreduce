package mapreduce

import "fmt"
import "os"
import "path"
import "log"
import "strconv"
import "encoding/json"
import "sort"
import "container/list"
import "bufio"
import "hash/fnv"

// A naive mapreduce library implementation
//
// The application provides an input file f, a Map and Reduce function,
// and the number of nMap and nReduce tasks.
//
// Split() splits the file f in nMap input files:
//    f-0, f-1, ..., f-<nMap-1>
// one for each Map job.
//
// DoMap() runs Map on each map file and produces nReduce files for a
// map file.  Thus, there will be nMap x nReduce files after all map
// jobs are done:
//    f-0-0, ..., f-0-0, f-0-<nReduce-1>, ...,
//    f-<nMap-1>-0, ... f-<nMap-1>-<nReduce-1>.
//
// DoReduce() collects <nReduce> reduce files from each map (f-*-<reduce>),
// and runs Reduce on those files.  This produces <nReduce> result files,
// which Merge() merges into a single output.

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Map and Reduce deal with <key, value> pairs:
type KeyValue struct {
	Key   string
	Value string
}

type MapReduce struct {
	job     Job // the job to execute
	nMap    int
	nReduce int
	file    string
}

func InitMapReduce(job Job) *MapReduce {
	mr := new(MapReduce)
	mr.job = job
	mr.nMap = job.NMap
	mr.nReduce = job.NReduce
	mr.file = job.InputPath

	return mr
}

// Name of the file that is the input for map job <MapJob>
func MapName(filePath string, MapJob int) string {
	dir := path.Dir(filePath)
	file := path.Base(filePath)
	return path.Join(dir, "mrtmp."+file+"-"+strconv.Itoa(MapJob))
}

// Split bytes of input file into nMap splits, but split only on white space
func (mr *MapReduce) Split(filePath string) {
	fmt.Printf("Split %s\n", filePath)
	infile, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Split: ", err)
	}
	defer infile.Close()
	fi, err := infile.Stat()
	if err != nil {
		log.Fatal("Split: ", err)
	}
	size := fi.Size()
	nchunk := size / int64(mr.nMap)
	nchunk += 1

	outfile, err := os.Create(MapName(filePath, 0))
	if err != nil {
		log.Fatal("Split: ", err)
	}
	writer := bufio.NewWriter(outfile)
	m := 1
	i := 0

	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		if int64(i) > nchunk*int64(m) {
			writer.Flush()
			outfile.Close()
			outfile, err = os.Create(MapName(filePath, m))
			writer = bufio.NewWriter(outfile)
			m += 1
		}
		line := scanner.Text() + "\n"
		writer.WriteString(line)
		i += len(line)
	}
	writer.Flush()
	outfile.Close()
}

func ReduceName(filePath string, MapJob int, ReduceJob int) string {
	mapFile := MapName(filePath, MapJob)
	dir := path.Dir(mapFile)
	file := path.Base(mapFile)

	return path.Join(dir, file+"-"+strconv.Itoa(ReduceJob))
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// Read split for job, call Map for that split, and create nreduce
// partitions.
func DoMap(JobNumber int, fileName string,
	nreduce int, Map func(string) *list.List) {
	name := MapName(fileName, JobNumber)
	file, err := os.Open(name)
	if err != nil {
		log.Fatal("DoMap: ", err)
	}
	fi, err := file.Stat()
	if err != nil {
		log.Fatal("DoMap: ", err)
	}
	size := fi.Size()
	fmt.Printf("DoMap: read split %s %d\n", name, size)
	b := make([]byte, size)
	_, err = file.Read(b)
	if err != nil {
		log.Fatal("DoMap: ", err)
	}
	file.Close()
	res := Map(string(b))
	// XXX a bit inefficient. could open r files and run over list once
	for r := 0; r < nreduce; r++ {
		file, err = os.Create(ReduceName(fileName, JobNumber, r))
		if err != nil {
			log.Fatal("DoMap: create ", err)
		}
		enc := json.NewEncoder(file)
		for e := res.Front(); e != nil; e = e.Next() {
			kv := e.Value.(KeyValue)
			if hash(kv.Key)%uint32(nreduce) == uint32(r) {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal("DoMap: marshall ", err)
				}
			}
		}
		file.Close()
	}
}

func MergeName(fileName string, ReduceJob int) string {
	dir := path.Dir(fileName)
	file := path.Base(fileName)
	return path.Join(dir, "mrtmp."+file+"-res-"+strconv.Itoa(ReduceJob))
}

// Read map outputs for partition job, sort them by key, call reduce for each
// key
func DoReduce(job int, fileName string, nmap int,
	Reduce func(string, *list.List) string) {
	kvs := make(map[string]*list.List)
	for i := 0; i < nmap; i++ {
		name := ReduceName(fileName, i, job)
		fmt.Printf("DoReduce: read %s\n", name)
		file, err := os.Open(name)
		if err != nil {
			log.Fatal("DoReduce: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			_, ok := kvs[kv.Key]
			if !ok {
				kvs[kv.Key] = list.New()
			}
			kvs[kv.Key].PushBack(kv.Value)
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	p := MergeName(fileName, job)
	file, err := os.Create(p)
	if err != nil {
		log.Fatal("DoReduce: create ", err)
	}
	enc := json.NewEncoder(file)
	for _, k := range keys {
		res := Reduce(k, kvs[k])
		enc.Encode(KeyValue{k, res})
	}
	file.Close()
}

func ResultName(filePath string) string {
	dir := path.Dir(filePath)
	file := path.Base(filePath)
	return path.Join(dir, "mrtmp."+file)
}

// Merge the results of the reduce jobs
// XXX use merge sort
func (mr *MapReduce) Merge() {
	DPrintf("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := MergeName(mr.file, i)
		fmt.Printf("Merge: read %s\n", p)
		file, err := os.Open(p)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := os.Create(ResultName(mr.file))
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s\t%s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}

func RemoveFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}

func (mr *MapReduce) CleanupFiles() {
	for i := 0; i < mr.nMap; i++ {
		RemoveFile(MapName(mr.file, i))
		for j := 0; j < mr.nReduce; j++ {
			RemoveFile(ReduceName(mr.file, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		RemoveFile(MergeName(mr.file, i))
	}
	RemoveFile("mrtmp." + mr.file)
}

// Run jobs sequentially.
func RunSequential(job Job,
	Map func(string) *list.List,
	Reduce func(string, *list.List) string) {
	mr := InitMapReduce(job)
	mr.Split(mr.file)
	for i := 0; i < mr.nMap; i++ {
		DoMap(i, mr.file, mr.nReduce, Map)
	}
	for i := 0; i < mr.nReduce; i++ {
		DoReduce(i, mr.file, mr.nMap, Reduce)
	}
	mr.Merge()
}

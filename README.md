go-mapreduce
============

Simple map-reduce implementation in Golang

## Limits ##
Lots of limits for the moment:

- single input file
- processes should be on the same machine for a shared local FS
- does not tolerate worker failure
- a single job at a moment

## Examples ##

### Word count ###

- Run a master
	`go run wc.go master localhost:7777`
- Run a worker
	`go run wc.go worker localhost:7777 localhost:7778`
- Submit the job
	`go run wc.go submit examples/bible.txt localhost:7777`

Alternatively the job can be run sequentially in a single process:

	`go run wc.go sequential examples/bible.txt`

Result file is in the same directory as the input

	`examples/mrtmp.bible.txt`


## TODOs ##

- Allow multiple jobs in parallel
- Decouple map and reduce function definition from worker, they should be part of the job definition and get submitted to workers
- Try out how to make use of `mesos` to run workers

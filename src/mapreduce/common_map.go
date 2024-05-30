package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	// Read the input file
	contents, err := os.ReadFile(inFile)
	checkError(err)

	// Call mapF to get the key-value intermediate pairs
	kvPairs := mapF(inFile, string(contents))

	// Create nReduce intermediate files
	outFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		// reduceName constructs the name of the intermediate file which map task
		// <mapTask> produces for reduce task <reduceTask>.
		// func reduceName(jobName string, mapTask int, reduceTask int) string
		outFile, err := os.Create(reduceName(jobName, mapTaskNumber, i))
		checkError(err)

		// Close the file when we're done using defer
		defer outFile.Close()
		outFiles[i] = outFile
		encoders[i] = json.NewEncoder(outFile)
	}

	// Distribute the key-value pairs to the appropriate files
	for _, kv := range kvPairs {
		// Use ihash function to determine which reduce task to send the key-value pair to
		reduceTaskNumber := ihash(kv.Key) % uint32(nReduce)
		err := encoders[reduceTaskNumber].Encode(&kv)
		checkError(err)

	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvMap := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		// reduceName constructs the name of the intermediate file which map task
		// <mapTask> produces for reduce task <reduceTask>.
		// func reduceName(jobName string, mapTask int, reduceTask int) string
		file, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		checkError(err)

		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	// Sort keys
	var keys []string
	for key := range kvMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Create Output File

	// mergeName constructs the name of the output file of reduce task <reduceTask>
	// func mergeName(jobName string, reduceTask int) string
	outFile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	checkError(err)

	defer outFile.Close()

	// Create JSON encoder
	enc := json.NewEncoder(outFile)

	// Call reduceF for each key and write the output
	for _, key := range keys {
		result := reduceF(key, kvMap[key])
		err := enc.Encode(KeyValue{Key: key, Value: result})
		checkError(err)

	}
}

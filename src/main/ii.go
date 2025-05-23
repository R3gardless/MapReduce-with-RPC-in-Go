package main

import (
	"fmt"
	"mapreduce"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// Function to determine the split condition (non-letter and non-number)
	splitFunc := func(c rune) bool {
		return !unicode.IsLetter(c)
	}

	// Split the input text into words
	words := strings.FieldsFunc(value, splitFunc)

	// Create key-value pairs(word, document)
	for _, word := range words {
		kv := mapreduce.KeyValue{word, document}
		res = append(res, kv)
	}

	return res
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {

	// Create a set to store unique document identifiers
	uniqueDocs := make(map[string]uint64)

	// Initialize the map with unique document
	for _, val := range values {
		uniqueDocs[val] = 1
	}

	// Convert map keys to a slice and sort
	var docList []string
	for docName := range uniqueDocs {
		docList = append(docList, docName)
	}
	sort.Strings(docList)

	// Return the count of unique documents and sorted list as a single string
	return strconv.Itoa(len(docList)) + " " + strings.Join(docList, ",")
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}

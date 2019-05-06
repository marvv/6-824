package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type sortKv []KeyValue
func (a sortKv) Len() int           { return len(a) }
func (a sortKv) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortKv) Less(i, j int) bool {
	if a[i].Key != a[j].Key {
		return a[i].Key < a[j].Key
	}
	return a[i].Value < a[j].Value
}


func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	//reduceName(jobName, m, reduceTask)
	kvList := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		inputFileName := reduceName(jobName, i, reduceTask)
		f, _ := os.Open(inputFileName)
		dec := json.NewDecoder(f)
		tmpKv := KeyValue{}

		for dec.Decode(&tmpKv) == nil {
			kvList = append(kvList, tmpKv)
		}
		f.Close()
	}

	sort.Sort(sortKv(kvList))
	fmt.Printf("kvlen %d\n", len(kvList))
	resultList := make([]KeyValue, 0)
	tmpMap := make(map[string][]string)
	keyList := make([]string,0)
	for _, kv := range kvList {
		if _, find := tmpMap[kv.Key] ; !find {
			keyList = append(keyList, kv.Key)
			tmpMap[kv.Key] = make([]string, 0)
		}
		tmpMap[kv.Key] = append(tmpMap[kv.Key], kv.Value)
	}

	for _, key := range keyList {
		res := reduceF(key, tmpMap[key])
		resultList = append(resultList, KeyValue{key, res})
	}

	outputFile, _ := os.Create(outFile)
	defer outputFile.Close()
	enc := json.NewEncoder(outputFile)
	for _, kv := range resultList {
		enc.Encode(&kv)
	}

}

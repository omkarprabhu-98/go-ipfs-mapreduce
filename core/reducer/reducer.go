package reducer

import (
	"context"
	"encoding/json"
	"fmt"
	core "github.com/ipfs/go-ipfs/core"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	// gorpc "github.com/libp2p/go-libp2p-gorpc"

	"github.com/omkarprabhu-98/go-ipfs-mapreduce/common"
)

type ReduceService struct {
	Node *core.IpfsNode
}

func (rs *ReduceService) Reduce(ctx context.Context, reduceInput common.ReduceInput, reduceOutput *common.ReduceOutput) error {
	log.Println("In Reduce")

	outputFileCid, _ := rs.doReduce(ctx, reduceInput.KvFileCids, reduceInput.MasterPeerId, reduceInput.ReducerNo, reduceInput.Round)
	log.Println("Reduce output ready")

	reduceOutput.ReducerNo = reduceInput.ReducerNo
	reduceOutput.OutputFileCid = outputFileCid
	log.Println("Pinged master with reduce output")
	// PREV-->  } ()
	return nil
}

func (rs *ReduceService) doReduce(ctx context.Context,
	kvFileCids []string, masterPeerId string,
	reducerNo int, round int) (string, error) {
	var kva []common.KeyValue
	var kv common.KeyValue
	// fill the map from all files
	for _, kvFileCid := range kvFileCids {
		kvFile, err := common.GetInTmpFile(ctx, rs.Node, kvFileCid)
		if err != nil {
			return "", err
		}
		os.Remove(kvFile.Name())
		dec := json.NewDecoder(kvFile)
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				log.Println("Cannot decode from KV file", err)
				return "", err
			}
			kva = append(kva, kv)
		}
	}
	log.Println("Reduce output map ready")

	// Create output file file
	filePath := fmt.Sprintf("%v-%v", masterPeerId, reducerNo)
	file, err := os.Create(filePath)
	if err != nil {
		log.Println("Unable to create output file", err)
		return "", err
	}
	defer os.Remove(filePath)
	log.Println("Reduce output file ready")

	var output map[string]string
	switch round {
	case 1:
		output = reducef1(kva)
	case 2:
		output = reducef2(kva)
	case 3:
		output = reducef3(kva)
	}

	// Call reduce and write to temp file
	for k, v := range output {
		_, err := fmt.Fprintf(file, "%s %s\n", k, v)
		if err != nil {
			log.Println("Unable to write output to file", err)
			return "", err
		}
	}

	log.Println("Output written to file")
	outfileCid, err := common.AddFile(ctx, rs.Node, filePath)
	if err != nil {
		return "", err
	}
	return outfileCid.String(), err
}

// Reduce 3
// <doc, word> <tf_idf>
func reducef3(kva []common.KeyValue) map[string]string {

	// [word][doc] = {count, total}
	df_t := make(map[string]map[string]string)
	corpus_ids := make(map[string]bool)

	for _, line := range kva {
		word := line.Key
		vals := strings.Split(line.Value, ";")
		doc := vals[0]
		_, ok := df_t[word]
		if !ok {
			df_t[word] = make(map[string]string)
			df_t[word][doc] = vals[1] + ";" + vals[2]
		}
		df_t[word][doc] = vals[1] + ";" + vals[2]
		corpus_ids[doc] = true
	}

	size := float64(len(corpus_ids))
	kv := make(map[string]string)

	for word, v := range df_t {
		for doc, val := range v {
			e := strings.Split(val, ";")
			count, _ := strconv.ParseFloat(e[0], 64)
			total, _ := strconv.ParseFloat(e[1], 64)
			tf_idf := (count / total) * math.Log10(size/float64(len(df_t[word])))
			kv[word+";"+doc] = fmt.Sprint(tf_idf)
		}
	}

	return kv
}

// Reduce 2
// <doc, word> <word_count, total_words>
func reducef2(kva []common.KeyValue) map[string]string {
	kvMap := make(map[string]string)
	wordCount := make(map[string]int)

	for _, kv := range kva {
		v, _ := strconv.Atoi(strings.Split(kv.Value, ";")[1])
		wordCount[kv.Key] += v
	}

	for _, kv := range kva {
		vals := strings.Split(kv.Value, ";")
		kvMap[kv.Key+";"+vals[0]] = vals[1] + ";" + strconv.Itoa(wordCount[kv.Key])
	}

	return kvMap
}

//Reduce 1
//<doc, word> <word_count>
func reducef1(kva []common.KeyValue) map[string]string {
	kvMap := make(map[string]int)
	for _, kv := range kva {
		v, _ := strconv.Atoi(kv.Value)
		kvMap[kv.Key] += v
	}
	kv := make(map[string]string)
	for k, v := range kvMap {
		kv[k] = strconv.Itoa(v)
	}
	return kv
}

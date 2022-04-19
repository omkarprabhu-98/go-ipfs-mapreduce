package reducer

import (
	"context"
	"encoding/json"
	"fmt"
	core "github.com/ipfs/go-ipfs/core"
	"log"
	"math"
	"strings"

	// "math"
	"os"
	"strconv"
	// "strings"

	// gorpc "github.com/libp2p/go-libp2p-gorpc"

	"github.com/omkarprabhu-98/go-ipfs-mapreduce/common"
)

type ReduceService struct {
	Node *core.IpfsNode
}

// PREV--> func (rs *ReduceService) Reduce(ctx context.Context, reduceInput common.ReduceInput, empty *common.Empty) error {
func (rs *ReduceService) Reduce(ctx context.Context, reduceInput common.ReduceInput, reduceOutput *common.ReduceOutput) error {
	log.Println("In Reduce")
	// PREV -->
	// go func () {
	// errors ignored as keeping this stateless
	// if master does not get a response after a duration it assumes the node/data
	// is lost and retries
	// ctx := context.Background()
	outputFileCid, _ := rs.doReduce(ctx, reduceInput.KvFileCids, reduceInput.MasterPeerId,
		reduceInput.ReducerNo, reduceInput.NoOfDocuments)
	log.Println("Reduce output ready")
	// PREV-->
	// peer, err := common.GetPeerFromId(reduceInput.MasterPeerId)
	// if err != nil {
	// 	log.Fatalln("Unable to get master peer")
	// }
	// rpcClient := gorpc.NewClient(rs.Node.PeerHost, common.ProtocolID)
	// if err := rpcClient.Call(peer.ID, common.MasterServiceName, common.MasterReduceOutputFuncName,
	// 	common.ReduceOutput{ReducerNo: reduceInput.ReducerNo, OutputFileCid: outputFileCid,},
	// 	&common.Empty{}); err != nil {
	// 	log.Fatalln("Err calling the master for reduce output", err)
	// }
	reduceOutput.ReducerNo = reduceInput.ReducerNo
	reduceOutput.OutputFileCid = outputFileCid
	log.Println("Pinged master with reduce output")
	// PREV-->  } ()
	return nil
}

func (rs *ReduceService) doReduce(ctx context.Context,
	kvFileCids []string,
	masterPeerId string, reducerNo int, documentCount int) (string, error) {
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

	// Call reduce and write to temp file
	for k, v := range reducef(kva, documentCount) {
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

func reducef(kva []common.KeyValue, documentCount int) map[string]string {
	//kvi := make(map[string]int)
	//for _, v := range(kva) {
	//	_, ok := kvi[v.Key]
	//	if !ok {
	//		kvi[v.Key] = 0
	//	}
	//	kvi[v.Key] += 1
	//}
	//kv := make(map[string]string)
	//for k, v := range(kvi) {
	//	kv[k] = strconv.Itoa(v)
	//}
	//return kv

	//[word][doc] -> count
	tf_c := make(map[string]map[string]int)
	//[word][doc] -> total
	tf_t := make(map[string]map[string]int)
	// word -> # of documents
	df := make(map[string]int)
	kv := make(map[string]string)

	for _, line := range kva {
		if line.Key != "" {
			keys := strings.Split(line.Key, common.Separator)
			word := keys[0]
			doc := keys[1]
			vals := strings.Split(line.Value, common.Separator)

			_, ok := tf_c[word]
			if !ok {
				tf_c[word] = make(map[string]int)
				tf_t[word] = make(map[string]int)
			}

			count, _ := strconv.Atoi(vals[0])
			total, _ := strconv.Atoi(vals[1])
			tf_c[word][doc] = tf_c[word][doc] + count
			tf_t[word][doc] = tf_t[word][doc] + total
			df[word]++
		}
	}

	for word, docs := range tf_c {
		for doc, count := range docs {
			tf_idf := (float64(count) / float64(tf_t[word][doc])) * math.Log10(float64(documentCount)/float64(df[word]))
			kv[word+common.Separator+doc] = fmt.Sprint(tf_idf)
		}
	}

	return kv
}

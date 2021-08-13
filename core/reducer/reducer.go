package reducer

import (
	"context"
	"log"
	"os"
	"encoding/json"
	"fmt"
	"sort"

	core "github.com/ipfs/go-ipfs/core"
	gorpc "github.com/libp2p/go-libp2p-gorpc"

	"github.com/omkarprabhu-98/go-ipfs-mapreduce/common"
)

type ReduceService struct {
	Node *core.IpfsNode
}

func (rs *ReduceService) Reduce(ctx context.Context, reduceInput common.ReduceInput, empty *common.Empty) error {
	log.Println("In Reduce")
	reducef, err := rs.loadReduceFunc(ctx, reduceInput.FuncFileCid)
	if err != nil {
		return err
	}
	log.Println("Extracted reduce func from file")
	go func () {
		// errors ignored as keeping this stateless
		// if master does not get a response after a duration it assumes the node/data 
		// is lost and retries
		ctx := context.Background()
		outputFileCid, _ := rs.doReduce(ctx, reducef, reduceInput.KvFileCids, reduceInput.MasterPeerId, reduceInput.ReducerNo)
		log.Println("Reduce output ready")
		peer, err := common.GetPeerFromId(reduceInput.MasterPeerId)
		if err != nil {
			log.Fatalln("Unable to get master peer")
		}
		rpcClient := gorpc.NewClient(rs.Node.PeerHost, common.ProtocolID)
		if err := rpcClient.Call(peer.ID, common.MasterServiceName, common.MasterReduceOutputFuncName,
			common.ReduceOutput{ReducerNo: reduceInput.ReducerNo, OutputFileCid: outputFileCid,},
			&common.Empty{}); err != nil {
			log.Fatalln("Err calling the master for reduce output", err)
		}
		log.Println("Pinged master with reduce output")
	} ()
	return nil
}

func  (rs *ReduceService) loadReduceFunc(ctx context.Context, fileCid string) (func(string, []string) string, error) {
	log.Println("Getting Reduce func from plugin file")
	p, err := common.GetPlugin(ctx, rs.Node, fileCid)
	if (err != nil) {
		return nil, err
	}
	log.Println("Plugin obtained")
	xreducef, err := p.Lookup(common.ReduceFuncName)
	if err != nil {
		log.Println("Cannot find Reduce in the file", err)
		return nil, err
	}
	reducef, ok := xreducef.(func(string, []string) string)
	if !ok {
		log.Println("Cannot find Reduce func with correct arguments", err)
		return nil, err
	}
	return reducef, nil
}

func (rs *ReduceService) doReduce(ctx context.Context, 
	reducef (func(string, []string) string), kvFileCids []string, 
	masterPeerId string, reducerNo int) (string, error) {
	kvMap := make(map[string][]string)
	var kv common.KeyValue
	// fill the map from all files
	for _, kvFileCid := range kvFileCids {
		kvFile, err := common.GetInTmpFile(ctx, rs.Node, kvFileCid)
		if (err != nil) {
			return "", err
		}
		os.Remove(kvFile.Name())
		dec := json.NewDecoder(kvFile)
		for dec.More() {
			err = dec.Decode(&kv)
			if (err != nil) {
				log.Println("Cannot decode from KV file", err)
				return "", err
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	log.Println("Reduce output map ready")
	// sort the kv map by key
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	log.Println("Reduce output keys sorted")

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
	for _, k := range keys {
		_, err := fmt.Fprintf(file, "%v %v\n", k, reducef(k, kvMap[k]))
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

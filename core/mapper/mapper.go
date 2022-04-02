package mapper

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"bufio"
	"encoding/json"
	"fmt"

	core "github.com/ipfs/go-ipfs/core"
	// gorpc "github.com/libp2p/go-libp2p-gorpc"

	"github.com/omkarprabhu-98/go-ipfs-mapreduce/common"
)

type MapService struct {
	Node *core.IpfsNode 
}

// PREV--> func (ms *MapService) Map(ctx context.Context, mapInput common.MapInput, empty *common.Empty) error {
func (ms *MapService) Map(ctx context.Context, mapInput common.MapInput, mapOutput *common.MapOutput) error {
	log.Println("In Map")
	mapf, err := ms.loadMapFunc(ctx, mapInput.FuncFileCid)
	if err != nil {
		return err
	}
	log.Println("Extracted map func from file")
	// PREV--> go func () {
	// errors ignored as keeping this stateless
	// if master does not get a response after a duration it assumes the node/data 
	// is lost and retries
	// PREV--> ctx := context.Background()
	kvList, _ := ms.doMap(ctx, mapf, mapInput.DataFileCid)
	log.Println("Map output ready")
	kvFileCids, err := ms.shuffleAndSave(ctx, kvList, mapInput.NoOfReducers, mapInput.DataFileCid)
	if err != nil {
		log.Fatalln("Unable to shuffle and save", err)
	}
	log.Println("Map output Cids ready")
	peer, err := common.GetPeerFromId(mapInput.MasterPeerId)
	if err != nil {
		log.Fatalln("Unable to get master peer")
		return err
	}
	if err := ms.Node.PeerHost.Connect(ctx, peer); err != nil {
		log.Fatalln("Unable to connect to master", err)
		return err
	}
	log.Println("Connected to master")
	// PREV--> rpcClient := gorpc.NewClient(ms.Node.PeerHost, common.ProtocolID)
	// if err := rpcClient.Call(peer.ID, common.MasterServiceName, common.MasterMapOutputFuncName,
	// 	common.MapOutput{DataFileCid: mapInput.DataFileCid, KvFileCids: kvFileCids,},
	// 	&common.Empty{}); err != nil {
	// 	log.Fatalln("Err calling the master for map output", err)
	// PREV--> }
	log.Println("Returned map output to master for ", mapInput.DataFileCid)
	mapOutput.DataFileCid = mapInput.DataFileCid
	mapOutput.KvFileCids = kvFileCids
	// PREV--> } ()
	return nil
}

func  (ms *MapService) loadMapFunc(ctx context.Context, fileCid string) (func(string, string) []common.KeyValue, error) {
	log.Println("Getting Map func from plugin file")
	p, err := common.GetPlugin(ctx, ms.Node, fileCid)
	if (err != nil) {
		return nil, err
	}
	log.Println("Plugin obtained")
	xmapf, err := p.Lookup(common.MapFuncName)
	if err != nil {
		log.Println("Cannot find Map in the file", err)
		return nil, err
	}
	mapf, ok := xmapf.(func(string, string) []common.KeyValue)
	if !ok {
		log.Println("Cannot find Map func with correct arguments", err)
		return nil, err
	}
	return mapf, nil
}

func (ms *MapService) doMap(ctx context.Context, mapf (func(string, string) []common.KeyValue), dataFileCid string) ([]common.KeyValue, error) {
	dataFile, err := common.GetInTmpFile(ctx, ms.Node, dataFileCid)
	if (err != nil) {
		return nil, err
	}
	defer os.Remove(dataFile.Name())
	content, err := ioutil.ReadAll(dataFile)
	if (err != nil) {
		log.Println("Unable to read data", err)
		return nil, err
	}
	log.Println("Read data as string")
	kvList := mapf(dataFile.Name(), string(content))
	return kvList, nil
}

func (ms *MapService) shuffleAndSave(ctx context.Context, kvList []common.KeyValue, noOfReducers int, dataFileCid string) ([]string, error) {
	// use io buffers to reduce disk I/O, which greatly improves
	// performance when running in containers with mounted volumes
	files := make([]*os.File, 0, noOfReducers)
	buffers := make([]*bufio.Writer, 0, noOfReducers)
	encoders := make([]*json.Encoder, 0, noOfReducers)
	kvFileCids := make([]string, 0, noOfReducers)
	// create temp files, use pid to uniquely identify this worker
	for i := 0; i < noOfReducers; i++ {
		filePath := fmt.Sprintf("%v-%v", dataFileCid, i)
		file, err := os.Create(filePath)
		if err != nil {
			log.Println("Cannot create file", filePath, err)
			return nil, err
		} 
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}
	log.Println(noOfReducers, "MapOutput files created successfully")
	// write map outputs to temp files
	for _, kv := range kvList {
		idx := common.Ihash(kv.Key) % noOfReducers
		err := encoders[idx].Encode(&kv)
		if (err != nil) {
			log.Println("Cannot encode kv ", err)
			return nil, err
		} 
	}
	log.Println("Output written to all file buffers")
	// flush file buffer to disk
	for _, buf := range buffers {
		err := buf.Flush()
		if (err != nil) {
			log.Println("Cannot flush buffer for file", err)
			return nil, err
		} 
	}
	log.Println("Flush to all files successful")
	for _, file := range files {
		cid, err := common.AddFile(ctx, ms.Node, file.Name())
		if err != nil {
			log.Println("Could not add file to ipfs", file.Name(), err)
			return nil, err
		}
		kvFileCids = append(kvFileCids, cid.String())
		os.Remove(file.Name())
	}
	return kvFileCids, nil
}


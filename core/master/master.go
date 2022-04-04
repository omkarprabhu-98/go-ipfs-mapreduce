package master

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"

	// "github.com/ipfs/go-ipfs-files"
	core "github.com/ipfs/go-ipfs/core"
	format "github.com/ipfs/go-ipld-format"
	// "github.com/ipfs/go-ipfs/core/coreunix"
	// "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs/core/coreapi"
	icorepath "github.com/ipfs/interface-go-ipfs-core/path"
	gorpc "github.com/libp2p/go-libp2p-gorpc"

	// multiaddr "github.com/multiformats/go-multiaddr"
	"github.com/omkarprabhu-98/go-ipfs-mapreduce/common"
)

type Master struct {
	Node             *core.IpfsNode
	RpcClient        *gorpc.Client
	NoOfReducers     int
	MrOutputFile     string
	DataFileCid      string
	DataFileBlocks   []string
	mu               sync.Mutex // protects all below
	MapStatus        Status
	BlockProviders   map[string][]string // datafileblock to peerid providers
	MapAllocation    map[string]string   // datafileblock to peerid allocated for map
	RMapAllocation   map[string]string   // reverse of map allocation
	MapOutput        map[string][]string // datafileblock to list of NoOfReducers cids for map output files
	ReduceStatus     Status
	ReduceOutput     map[int]string
	ReduceAllocation map[int]string   // number to peerid
	ReduceFileMap    map[int][]string // number to list of cids as per map output
}

type Status struct {
	Complete int
	Total    int
}

type MapTask struct {
	Cid     cid.Cid
	Retries int
}

type ReduceTask struct {
	Index   int
	Retries int
}

func (master *Master) RunMapReduce(ctx context.Context) {

	// Get blocks for the input file
	path := icorepath.New(master.DataFileCid)
	nodeCoreApi, err := coreapi.NewCoreAPI(master.Node)
	if err != nil {
		log.Fatalln(err)
	}
	dagnode, err := nodeCoreApi.ResolveNode(ctx, path)
	if err != nil {
		log.Fatalln("Unable to get the dag node", err)
	}
	dataBlocks := dagnode.Links()
	if len(dataBlocks) == 0 {
		rootCid, err := cid.Decode(master.DataFileCid)
		if err != nil {
			log.Fatalln("Unable to decode cid", err)
		}
		dataBlocks = append(dataBlocks, &format.Link{Cid: rootCid})
	}
	log.Println("Found", len(dataBlocks), "links for root cid", master.DataFileCid)

	// Init state variables
	master.RpcClient = gorpc.NewClient(master.Node.PeerHost, common.ProtocolID)
	master.MapStatus.Total = len(dataBlocks)
	master.MapStatus.Complete = 0
	master.ReduceStatus.Total = master.NoOfReducers
	master.ReduceStatus.Complete = 0

	// Starting Map
	mapTask := make(chan MapTask)
	wg := new(sync.WaitGroup)
	maxRange := common.MaxGoRoutines
	if len(dataBlocks) < maxRange {
		maxRange = len(dataBlocks)
	}
	for i := 0; i < maxRange; i++ {
		wg.Add(1)
		go master.MapHandler(ctx, mapTask, wg)
	}
	for _, link := range dataBlocks {
		log.Println("Sub block:", link)
		master.DataFileBlocks = append(master.DataFileBlocks, link.Cid.String())
		mapTask <- MapTask{link.Cid, 0}
	}
	close(mapTask)
	wg.Wait()

	log.Println("MAP COMPLETE")
	// Starting Reduce
	// master.startReduce()
	reduceTask := make(chan ReduceTask)
	done := make(chan bool, 1)
	peerChan := make(chan string, 100)

	go master.fillPeerChan(peerChan, done)
	for i := 0; i < maxRange; i++ {
		wg.Add(1)
		go master.ReduceHandler(ctx, reduceTask, peerChan, wg)
	}
	for i := 0; i < master.NoOfReducers; i++ {
		reduceTask <- ReduceTask{i, 0}
	}
	close(reduceTask)
	wg.Wait()
	done <- true
	log.Println("REDUCE COMPLETE")
	master.combineOutput()
}

func (master *Master) fillPeerChan(peerChan chan string, done chan bool) {
	for _, peerIdList := range master.BlockProviders {
		if len(done) == cap(done) {
			<-done
			return
		}
		for _, peerId := range peerIdList {
			if len(done) == cap(done) {
				<-done
				return
			}
			peerChan <- peerId
		}
	}
}

func (master *Master) MapHandler(ctx context.Context, mapTask chan MapTask, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range mapTask {
		if master.processBlock(ctx, task.Cid) != nil {
			if task.Retries < common.MaxRetries {
				task.Retries += 1
				mapTask <- task
			}
		}
	}
}

func (master *Master) ReduceHandler(ctx context.Context, reduceTask chan ReduceTask, peerChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for task := range reduceTask {
		if master.processReduce(ctx, peerChan, task.Index) != nil {
			if task.Retries < common.MaxRetries {
				task.Retries += 1
				reduceTask <- task
			}
		}
	}
}

func (master *Master) processBlock(ctx context.Context, blockCid cid.Cid) error {
	blockCidString := blockCid.String()
	log.Println("Processing block", blockCidString)
	nodeCoreApi, err := coreapi.NewCoreAPI(master.Node)
	if err != nil {
		log.Fatalln("Cannot get the core API", err)
		return err
	}
	ch, err := nodeCoreApi.Dht().FindProviders(ctx, icorepath.New(blockCidString))
	if err != nil {
		log.Fatalln("Unable to get providers", err)
		return err
	}
	// ch := master.Node.DHT.FindProvidersAsync(ctx, blockCid, common.NoOfProviders)
	set := make(map[string]struct{})
	var provs []string
	for peer := range ch {
		if _, ok := set[peer.ID.String()]; !ok {
			provs = append(provs, peer.ID.String())
		}
	}
	// TODO find some better strategy
	master.BlockProviders[blockCidString] = make([]string, len(provs))
	perm := rand.Perm(len(provs))
	for i, v := range perm {
		master.BlockProviders[blockCidString][v] = provs[i]
	}
	log.Println("Found", len(master.BlockProviders[blockCidString]), "providers for", blockCidString)
	for _, peerId := range master.BlockProviders[blockCidString] {
		peer, err := common.GetPeerFromId(peerId)
		if err != nil {
			log.Println("Unable to create peer f", err)
			continue
		}
		if err := master.Node.PeerHost.Connect(ctx, peer); err != nil {
			log.Println("Unable to connect to peer for map", err)
			master.BlockProviders[blockCidString] = master.BlockProviders[blockCidString][1:]
			continue
		}
		var mapOutput common.MapOutput
		if err = master.RpcClient.Call(peer.ID, common.MapServiceName, common.MapFuncName,
			common.MapInput{NoOfReducers: master.NoOfReducers,
				DataFileCid: blockCidString, MasterPeerId: master.Node.Identity.String()},
			// PREV--> &common.Empty{}); err != nil {
			&mapOutput); err != nil {
			log.Println("Unable to call peer for map", err)
			master.BlockProviders[blockCidString] = master.BlockProviders[blockCidString][1:]
			continue
		}
		log.Println("Allocated", blockCidString, "to peer: ", peer)
		master.MapAllocation[blockCidString] = peerId
		return master.processMapOutput(ctx, mapOutput)
	}
	return errors.New("unable to find a provider")
	// PREV-->
	// if _, ok := master.MapAllocation[blockCidString]; !ok {
	// 	log.Fatalln("Unable to find map allocation for", blockCidString)
	// }
}

// exported via gorpc
// PREV--> func (master *Master) ProcessMapOutput(ctx context.Context, mapOutput common.MapOutput, empty *common.Empty) error {
func (master *Master) processMapOutput(ctx context.Context, mapOutput common.MapOutput) error {
	master.mu.Lock()
	defer master.mu.Unlock()
	log.Println(mapOutput)
	master.MapOutput[mapOutput.DataFileCid] = mapOutput.KvFileCids
	for i, v := range mapOutput.KvFileCids {
		master.ReduceFileMap[i] = append(master.ReduceFileMap[i], v)
	}
	master.MapStatus.Complete += 1
	log.Println("Map output obtained for", mapOutput.DataFileCid)

	// PREV -->
	// if master.MapStatus.Complete == master.MapStatus.Total {
	// 	go master.startReduce()
	// }
	return nil
}

// PREV-->
// func (master *Master) startReduce() {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	log.Println("All Map outputs obtained, Starting Reduce...")
// 	// TODO find a better way
// 	master.mu.Lock()
// 	defer master.mu.Unlock()
// 	for i := 0; i < master.NoOfReducers; i++ {
// 		for  _, peerIdList := range master.BlockProviders {
// 			if _, ok := master.ReduceAllocation[i]; ok {
// 				break;
// 			}
// 			for _, peerId := range peerIdList {
// 				peer, err := common.GetPeerFromId(peerId)
// 				if err != nil {
// 					log.Println("Unable to create peer f", err)
// 					continue
// 				}
// 				if err := master.Node.PeerHost.Connect(ctx, peer); err != nil {
// 					log.Println("Unable to connect to peer for reduce", err)
// 					continue
// 				}
// 				if err := master.RpcClient.Call(peer.ID, common.ReduceServiceName, common.ReduceFuncName,
// 					common.ReduceInput{FuncFileCid: master.ReduceFuncFileCid, KvFileCids: master.ReduceFileMap[i],
// 						ReducerNo: i, MasterPeerId: master.Node.Identity.String(),},
// 					&common.Empty{}); err != nil {
// 					log.Println("Unable to call peer for reduce", err)
// 					continue
// 				}
// 				log.Println("Peer", peer.ID, "mapped to reducer no", i)
// 				master.ReduceAllocation[i] = peerId
// 				break;
// 			}
// 		}
// 	}
// 	// check
// 	if len(master.ReduceAllocation) != master.NoOfReducers {
// 		log.Fatalln("Unable to find the required reducers")
// 	}
// }

func (master *Master) processReduce(ctx context.Context, peerChan chan string, reduceIndex int) error {
	for peerId := range peerChan {
		peer, err := common.GetPeerFromId(peerId)
		if err != nil {
			log.Println("Unable to create peer f", err)
			continue
		}
		if err := master.Node.PeerHost.Connect(ctx, peer); err != nil {
			log.Println("Unable to connect to peer for reduce", err)
			continue
		}
		var reduceOutput common.ReduceOutput
		if err := master.RpcClient.Call(peer.ID, common.ReduceServiceName, common.ReduceFuncName,
			common.ReduceInput{KvFileCids: master.ReduceFileMap[reduceIndex],
				ReducerNo: reduceIndex, MasterPeerId: master.Node.Identity.String()},
			// &common.Empty{}); err != nil {
			&reduceOutput); err != nil {
			log.Println("Unable to call peer for reduce", err)
			continue
		}
		log.Println("Peer", peer.ID, "mapped to reducer no", reduceIndex)
		master.ReduceAllocation[reduceIndex] = peerId
		peerChan <- peerId
		return master.processReduceOutput(ctx, reduceOutput)
	}
	return errors.New("unable to perform reduce")
}

// exported via gorpc service
// func (master *Master) ProcessReduceOutput(ctx context.Context, reduceOutput common.ReduceOutput, empty *common.Empty) error {
func (master *Master) processReduceOutput(ctx context.Context, reduceOutput common.ReduceOutput) error {
	master.mu.Lock()
	defer master.mu.Unlock()
	master.ReduceOutput[reduceOutput.ReducerNo] = reduceOutput.OutputFileCid
	master.ReduceStatus.Complete += 1
	log.Println("Reduce output obtained for", reduceOutput.ReducerNo)
	// PREV-->
	// if master.ReduceStatus.Complete == master.ReduceStatus.Total {
	// 	go master.combineOutput()
	// }
	return nil
}

func (master *Master) combineOutput() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.Println("All Reduce output obtained, printing output")

	master.mu.Lock()
	defer master.mu.Unlock()

	f, _ := os.OpenFile(master.MrOutputFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	defer f.Close()

	for _, fileCid := range master.ReduceOutput {
		file, err := common.GetInTmpFile(ctx, master.Node, fileCid)
		if err != nil {
			log.Println("Error reading reduce output file", err)
		}
		defer os.Remove(file.Name())
		content, _ := ioutil.ReadAll(file)
		f.WriteString(fmt.Sprintf("%s\n", content))
	}
	log.Println("Map Reduce complete for", master.DataFileCid)
}

func (master *Master) GetMapStatus() Status {
	master.mu.Lock()
	defer master.mu.Unlock()
	return master.MapStatus
}

func (master *Master) GetReduceStatus() Status {
	master.mu.Lock()
	defer master.mu.Unlock()
	return master.ReduceStatus
}

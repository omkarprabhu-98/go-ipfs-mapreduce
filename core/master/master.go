package master

import (
	"context"
	"io/ioutil"
	"log"
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
	peerstore "github.com/libp2p/go-libp2p-core/peer"
	gorpc "github.com/libp2p/go-libp2p-gorpc"

	// multiaddr "github.com/multiformats/go-multiaddr"

	"github.com/omkarprabhu-98/go-ipfs-mapreduce/common"
)


type Master struct {
	Node               *core.IpfsNode
	RpcClient          *gorpc.Client
	MapFuncFilePath    string
	ReduceFuncFilePath string
	MapFuncFileCid     string
	ReduceFuncFileCid  string
	NoOfReducers       int
	DataFileCid        string
	DataFileBlocks     []string
	mu                 sync.Mutex // protects all below
	MapStatus	       Status
	BlockProviders     map[string][]peerstore.AddrInfo
	MapAllocation      map[string]peerstore.AddrInfo // datafileblock to peerid
	MapOutput          map[string][]string // datafileblock to list of NoOfReducers cids for map output files
	ReduceStatus       Status
	ReduceOutput       map[int]string 
	ReduceAllocation   map[int]peerstore.AddrInfo // number to peerid
	ReduceFileMap      map[int][]string // number to list of cids as per map output
}

type Status struct {
	Complete int
	Total    int
}

func (master *Master) RunMapReduce(ctx context.Context) {
	mapFuncFileRootCid, err := common.AddFile(ctx, master.Node, master.MapFuncFilePath)
	if (err != nil) {
		log.Fatalln(err)
	}
	master.MapFuncFileCid = mapFuncFileRootCid.String()	
	reduceFuncFileRootCid, err := common.AddFile(ctx, master.Node, master.ReduceFuncFilePath)
	if (err != nil) {
		log.Fatalln(err)
	}
	master.ReduceFuncFileCid = reduceFuncFileRootCid.String()	
	log.Println("Added Map reduce func file to ipfs")
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
	
	master.RpcClient = gorpc.NewClient(master.Node.PeerHost, common.ProtocolID)
	master.MapStatus.Total = len(dataBlocks)
	master.MapStatus.Complete = 0
	master.ReduceStatus.Total = master.NoOfReducers
	master.ReduceStatus.Complete = 0

 	for _, link := range dataBlocks {
		master.DataFileBlocks = append(master.DataFileBlocks, link.Cid.String())
		// TODO check if this is feasible
		go master.processBlock(ctx, link.Cid)
	}
}

func (master *Master) processBlock(ctx context.Context, blockCid cid.Cid) {
	ch := master.Node.DHTClient.FindProvidersAsync(ctx, blockCid, common.NoOfProviders)
	blockCidString := blockCid.String()
	log.Println("Processing block", blockCidString)
	for peer := range ch {
		master.BlockProviders[blockCidString] = append(master.BlockProviders[blockCidString], peer)
	}
	log.Println("Found", len(master.BlockProviders[blockCidString]), "providers for", blockCidString)
	for _, peer := range master.BlockProviders[blockCidString] {
		if err := master.Node.PeerHost.Connect(ctx, peer); err != nil {
			log.Println("Unable to connect to peer for map", err)
			master.BlockProviders[blockCidString] = master.BlockProviders[blockCidString][1:]
			continue
		}
		err := master.RpcClient.Call(peer.ID, common.MapServiceName, common.MapFuncName,
			common.MapInput{FuncFileCid: master.MapFuncFileCid, NoOfReducers: master.NoOfReducers, 
				DataFileCid: blockCidString, MasterPeerId: master.Node.Identity.String(),}, 
			&common.Empty{})
		if (err != nil) {
			log.Println("Unable to call peer for map", err)
			master.BlockProviders[blockCidString] = master.BlockProviders[blockCidString][1:]
			continue
		}
		log.Println("Allocated", blockCidString, "to peer: ", peer)
		master.MapAllocation[blockCidString] = peer
		break
	}
	if _, ok := master.MapAllocation[blockCidString]; !ok {
		log.Fatalln("Unable to find map allocation for", blockCidString)
	}
}

// exported via gorpc
func (master *Master) ProcessMapOutput(ctx context.Context, mapOutput common.MapOutput, empty *common.Empty) error {
	master.mu.Lock()
	defer master.mu.Unlock()
	log.Println(mapOutput)
	master.MapOutput[mapOutput.DataFileCid] = mapOutput.KvFileCids
	for i, v := range mapOutput.KvFileCids {
		master.ReduceFileMap[i] = append(master.ReduceFileMap[i], v)
	}
	master.MapStatus.Complete += 1
	log.Println("Map output obtained for", mapOutput.DataFileCid)

	if master.MapStatus.Complete == master.MapStatus.Total {
		go master.startReduce() 
	}
	return nil
}

func (master *Master) startReduce() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.Println("All Map outputs obtained, Starting Reduce...")
	// TODO find a better way
	master.mu.Lock()
	defer master.mu.Unlock()
	for i := 0; i < master.NoOfReducers; i++ {
		for  _, peerList := range master.BlockProviders {
			if _, ok := master.ReduceAllocation[i]; ok {
				break;
			}
			for _, peer := range peerList {
				if err := master.Node.PeerHost.Connect(ctx, peer); err != nil {
					log.Println("Unable to connect to peer for reduce", err)
					continue
				}
				err := master.RpcClient.Call(peer.ID, common.ReduceServiceName, common.ReduceFuncName,
					common.ReduceInput{FuncFileCid: master.ReduceFuncFileCid, KvFileCids: master.ReduceFileMap[i],
						ReducerNo: i, MasterPeerId: master.Node.Identity.String(),}, 
					&common.Empty{})
				if (err != nil) {
					log.Println("Unable to call peer for reduce", err)
					continue
				}
				log.Println("Peer", peer.ID, "mapped to reducer no", i)
				master.ReduceAllocation[i] = peer
				break;
			}
		}
	}
	// check 
	if len(master.ReduceAllocation) != master.NoOfReducers {
		log.Fatalln("Unable to find the required reducers")
	}
}

// exported via gorpc service
func (master *Master) ProcessReduceOutput(ctx context.Context, reduceOutput common.ReduceOutput, empty *common.Empty) error {
	master.mu.Lock()
	defer master.mu.Unlock()
	master.ReduceOutput[reduceOutput.ReducerNo] = reduceOutput.OutputFileCid
	master.ReduceStatus.Complete += 1
	log.Println("Reduce output obtained for", reduceOutput.ReducerNo)
	if master.ReduceStatus.Complete == master.ReduceStatus.Total {
		go master.combineOutput() 
	}
	return nil
}

func (master *Master) combineOutput() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log.Println("All Reduce output obtained, printing output")

	master.mu.Lock()
	defer master.mu.Unlock()
	// print for now
	for _, fileCid := range master.ReduceOutput {
		file, err := common.GetInTmpFile(ctx, master.Node, fileCid)
		if err != nil {
			log.Println("Error reading reduce output file", err)
		}
		defer os.Remove(file.Name())
		content, _ := ioutil.ReadAll(file)
		log.Println(string(content))
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
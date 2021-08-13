package mapreduce

import (
	core "github.com/ipfs/go-ipfs/core"
	"github.com/libp2p/go-libp2p-core/protocol"
	gorpc "github.com/libp2p/go-libp2p-gorpc"	

	"github.com/omkarprabhu-98/go-ipfs-mapreduce/core/mapper"
	"github.com/omkarprabhu-98/go-ipfs-mapreduce/core/master"
	"github.com/omkarprabhu-98/go-ipfs-mapreduce/core/reducer"
)

var protocolID = protocol.ID("/ipfs/mapreduce")

func RegisterProtocol(node *core.IpfsNode) error {
	mapService := mapper.MapService{Node: node}
	reduceService := reducer.ReduceService{Node: node}
	rpcHost := gorpc.NewServer(node.PeerHost, protocolID)

	rpcHost.Register(&mapService)
	rpcHost.Register(&reduceService)
	return nil
}

func InitMaster(node *core.IpfsNode, mapFuncFilePath string, reduceFuncFilePath string, 
	noOfReducers int, dataFileCid string) (*master.Master, error) {
	master := master.Master{
		Node: node, MapFuncFilePath: mapFuncFilePath, ReduceFuncFilePath: reduceFuncFilePath, 
		DataFileCid: dataFileCid, BlockProviders: make(map[string][]string), 
		MapAllocation: make(map[string]string),
		ReduceAllocation: make(map[int]string),
		MapOutput: make(map[string][]string),
		ReduceFileMap: make(map[int][]string),
		ReduceOutput: make(map[int]string),
		NoOfReducers: noOfReducers,
	}
	rpcHost := gorpc.NewServer(node.PeerHost, protocolID)
	rpcHost.Register(&master)
	return &master, nil
}

package common

import (
	"context"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"plugin"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-files"
	core "github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	"github.com/ipfs/go-ipfs/core/coreunix"
	// "github.com/ipfs/interface-go-ipfs-core/options"
	icorepath "github.com/ipfs/interface-go-ipfs-core/path"
	peerstore "github.com/libp2p/go-libp2p-core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

func AddFile(ctx context.Context, node *core.IpfsNode, filePath string) (cid.Cid, error) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Unable to open file", filePath, err)
		return cid.Cid{}, err
	}
	adder, err := coreunix.NewAdder(ctx, node.Pinning, node.Blockstore, node.DAG)
	if err != nil {
		log.Println("Unable to get ipfs adder", err)
		return cid.Cid{}, err
	}
	fileReader := files.NewReaderFile(file)
	rootNode, err := adder.AddAllAndPin(fileReader)
	if err != nil {
		log.Println("Unable to get Add and pin", err)
		return cid.Cid{}, err
	}
	// nodeCoreApi, err := coreapi.NewCoreAPI(node)
	// if err != nil {
	//     log.Println("Cannot get the core API", err)
	// 	return cid.Cid{}, err
	// }
	// rootNode, _ := nodeCoreApi.Unixfs().Add(ctx, fileReader)
	// for _, link := range rootNode.Links() {
	// 	log.Println(link)
	// 	if err := nodeCoreApi.Dht().Provide(ctx, icorepath.New(rootNode.Cid().String())); err != nil {
	// 		log.Println("Unable to provide this link", err)
	// 		return cid.Cid{}, err
	// 	}
	// }
	return rootNode.Cid(), nil
}

func GetPlugin(ctx context.Context, node *core.IpfsNode, fileCid string) (*plugin.Plugin, error) {
	// TODO find some better way to do this
	rootNode, err := GetFile(ctx, node, fileCid)
	if err != nil {
		return nil, err
	}
	// reopening plugin gives error find a way to avoid this
	// using the same file avoid the problem above
	file, err := os.OpenFile("plugin-"+fileCid+".so", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Unable to create file", err)
		return nil, err
	}
	err = files.WriteTo(rootNode, file.Name())
	if err != nil {
		log.Println("Could not write out the fetched file from IPFS", err)
		os.Remove(file.Name())
		return nil, err
	}
	defer os.Remove(file.Name())
	p, err := plugin.Open(file.Name())
	if p != nil {
		return p, nil
	}
	log.Println("cannot load plugin", err)
	return nil, err
}

func GetInTmpFile(ctx context.Context, node *core.IpfsNode, fileCid string) (*os.File, error) {
	rootNode, err := GetFile(ctx, node, fileCid)
	if err != nil {
		return nil, err
	}
	tmpFile, err := WriteToTmp(ctx, rootNode)
	if err != nil {
		return nil, err
	}
	return tmpFile, nil
}

func WriteToTmp(ctx context.Context, rootNode files.Node) (*os.File, error) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "pre-")
	if err != nil {
		log.Println("Cannot create temporary file", err)
		return nil, err
	}
	log.Println("Created a Temp File: " + tmpFile.Name())
	err = files.WriteTo(rootNode, tmpFile.Name())
	if err != nil {
		log.Println("Could not write out the fetched file from IPFS", err)
		os.Remove(tmpFile.Name())
		return nil, err
	}
	return tmpFile, err
}

func GetFile(ctx context.Context, node *core.IpfsNode, fileCid string) (files.Node, error) {
	path := icorepath.New(fileCid)
	nodeCoreApi, err := coreapi.NewCoreAPI(node)
	if err != nil {
		log.Println("Cannot get the core API", err)
		return nil, err
	}
	rootNode, err := nodeCoreApi.Unixfs().Get(ctx, path)
	if err != nil {
		log.Println("Could not get file", err)
		return nil, err
	}
	return rootNode, nil
}

func Ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GetPeerFromId(peerId string) (peerstore.AddrInfo, error) {
	addr, err := multiaddr.NewMultiaddr("/p2p/" + peerId)
	if err != nil {
		log.Println("Unable to get multiaddr", err)
		return peerstore.AddrInfo{}, err
	}
	peer, err := peerstore.AddrInfoFromP2pAddr(addr)
	if err != nil {
		log.Println("Unable to obtain peer", err)
		return peerstore.AddrInfo{}, err
	}
	return *peer, nil
}

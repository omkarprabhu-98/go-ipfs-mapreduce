package common

import (
	"context"
	"log"
	"os"
    "plugin"
    "io/ioutil"
	"hash/fnv"

	"github.com/ipfs/go-ipfs-files"
	core "github.com/ipfs/go-ipfs/core"
	icorepath "github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs/core/coreunix"
	"github.com/ipfs/go-ipfs/core/coreapi"
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
    return rootNode.Cid(), nil
}

func GetPlugin(ctx context.Context, node *core.IpfsNode, fileCid string) (*plugin.Plugin, error) {
	// TODO find some better way to do this
	tmpFile, err := GetInTmpFile(ctx, node, fileCid)
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpFile.Name())
	p, err := plugin.Open(tmpFile.Name())
	// reopening plugin gives error find a way to avoid this
	if p != nil {
		return p, nil
	}
	log.Println("cannot load plugin", err)
	return nil, err
}

func GetInTmpFile(ctx context.Context, node *core.IpfsNode, fileCid string) (*os.File, error) {
	rootNode, err := GetFile(ctx, node, fileCid)
	if (err != nil) {
		return nil, err
	}
	tmpFile, err := WriteToTmp(ctx, rootNode)
	if (err != nil) {
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
package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"bufio"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
	"log"

	mapreduce "github.com/omkarprabhu-98/go-ipfs-mapreduce"
	common "github.com/omkarprabhu-98/go-ipfs-mapreduce/common"

	// "github.com/libp2p/go-libp2p"
	// "github.com/ipfs/go-cid"
	core "github.com/ipfs/go-ipfs/core"
	// "github.com/libp2p/go-libp2p-core/host"
	// "github.com/libp2p/go-libp2p-core/network"
	// "github.com/libp2p/go-libp2p-core/peer"
	peerstore "github.com/libp2p/go-libp2p-core/peer"
	// multiaddr "github.com/multiformats/go-multiaddr"
	// icorepath "github.com/ipfs/interface-go-ipfs-core/path"

	// corenet "github.com/ipfs/go-ipfs/core/corehttp"
	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"
	// "github.com/ipfs/go-ipfs/core"
	// "github.com/ipfs/go-ipfs/core/coreapi"
	// icore "github.com/ipfs/interface-go-ipfs-core"
	config "github.com/ipfs/go-ipfs-config"
	libp2p "github.com/ipfs/go-ipfs/core/node/libp2p"
	"github.com/ipfs/go-ipfs/plugin/loader" // This package is needed so that all the preloaded plugins are loaded automatically
)

func shard(node *core.IpfsNode, inputFile string, N int) ([]string, int) {
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	sc := bufio.NewScanner(file)
	var fileCids []string
	lines := 0
	var tmpFile *os.File
	i := 0
	// Read through 'tokens' until an EOF is encountered.
	for sc.Scan() {
		lines += 1
		if (i == N) {
			i = 0;
			tmpFile.Close()
			cid, _ := common.AddFile(context.Background(), node, tmpFile.Name())
			fileCids = append(fileCids, cid.String())
			fmt.Println("Input file added", cid)
			// add file to ipfs
			os.Remove(tmpFile.Name())
		}
		if (i == 0) {
			tmpFile, _ = ioutil.TempFile(os.TempDir(), "prefix-")
		}
		txt := sc.Text() + "\n"
		if _, err = tmpFile.Write([]byte(txt)); err != nil {
			log.Fatal("Failed to write to temporary file", err)
		}
		i += 1
	}
	tmpFile.Close()
	cid, _ := common.AddFile(context.Background(), node, tmpFile.Name())
	fileCids = append(fileCids, cid.String())
	fmt.Println("Input file added", cid)
	// add file to ipfs
	os.Remove(tmpFile.Name())

	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
	return fileCids, lines
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fmt.Println("Spawning ephemeral ipfs node")
	node, err := spawnEphemeral(ctx)
	if err != nil {
		panic(fmt.Errorf("failed to spawn ephemeral node: %s", err))
	}
	err = mapreduce.RegisterProtocol(node)
	if err != nil {
		panic(fmt.Errorf("failed to register map reduce protocol: %s", err))
	}
	if len(os.Args) != 2 {
		n, _ := strconv.Atoi(os.Args[1])
		fmt.Println(n)
		N, _ := strconv.Atoi(os.Args[len(os.Args) - 1])
		master, err := mapreduce.InitMaster(node, n, os.Args[2], os.Args[3], os.Args[4 : len(os.Args) - 1], N)
		if err != nil {
			panic(fmt.Errorf("failed to init master: %s", err))
		}
		master.RunMapReduce(ctx)
		ticker := time.NewTicker(5 * time.Second)
		quit := make(chan struct{})
		go func() {
			for {
				select {
				case <-ticker.C:
					fmt.Println("MapStatus:", master.GetMapStatus())
					redStatus := master.GetReduceStatus()
					fmt.Println("ReduceStatus:", redStatus)
					if redStatus.Complete == redStatus.Total {
						quit <- struct{}{}
					}
				case <-quit:
					ticker.Stop()
					return
				}
			}
		}()
	} else {
		// cid, _ := common.AddFile(ctx, node, os.Args[1])
		// fmt.Println("Input file added", cid)
		fileCids, lines := shard(node, os.Args[1], 4)
		for _, i := range(fileCids) {
			fmt.Println(i)
		}
		fmt.Println(lines)
	}
	// wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	fmt.Println("Received signal, shutting down...")
}

func setupPlugins(externalPluginsPath string) error {
	// Load any external plugins if available on externalPluginsPath
	plugins, err := loader.NewPluginLoader(filepath.Join(externalPluginsPath, "plugins"))
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	// Load preloaded and external plugins
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

func createTempRepo(ctx context.Context) (string, error) {
	repoPath, err := ioutil.TempDir("", "ipfs-shell")
	if err != nil {
		return "", fmt.Errorf("failed to get temp dir: %s", err)
	}
	fmt.Println(repoPath)
	// Create a config with default options and a 2048 bit key
	cfg, err := config.Init(ioutil.Discard, 2048)
	if err != nil {
		return "", err
	}

	// Create the repo with the config
	err = fsrepo.Init(repoPath, cfg)
	if err != nil {
		return "", fmt.Errorf("failed to init ephemeral node: %s", err)
	}

	return repoPath, nil
}

// Spawns a node to be used just for this run (i.e. creates a tmp repo)
func spawnEphemeral(ctx context.Context) (*core.IpfsNode, error) {
	if err := setupPlugins(""); err != nil {
		return nil, err
	}

	// Create a Temporary Repo
	repoPath, err := createTempRepo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp repo: %s", err)
	}

	// Spawning an ephemeral IPFS node
	return createNode(ctx, repoPath)
}

// Creates an IPFS node and returns its coreAPI
func createNode(ctx context.Context, repoPath string) (*core.IpfsNode, error) {
	// Open the repo
	repo, err := fsrepo.Open(repoPath)
	if err != nil {
		return nil, err
	}

	// Construct the node
	nodeOptions := &core.BuildCfg{
		Online:  true,
		Routing: libp2p.DHTOption, // This option sets the node to be a full DHT node (both fetching and storing DHT Records)
		// Routing: libp2p.DHTClientOption, // This option sets the node to be a client DHT node (only fetching records)
		Repo: repo,
	}

	node, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		return nil, err
	}

	peer := node.PeerHost

	peerInfo := peerstore.AddrInfo{
		ID:    peer.ID(),
		Addrs: peer.Addrs(),
	}
	addrs, err := peerstore.AddrInfoToP2pAddrs(&peerInfo)
	fmt.Println("libp2p node address:", addrs)

	// Attach the Core API to the constructed node
	return node, nil
}

// Spawns a node on the default repo location, if the repo exists
func spawnDefault(ctx context.Context) (*core.IpfsNode, error) {
	defaultPath, err := config.PathRoot()
	if err != nil {
		// shouldn't be possible
		return nil, err
	}

	if err := setupPlugins(defaultPath); err != nil {
		return nil, err

	}

	return createNode(ctx, defaultPath)
}

# Go IPFS MapReduce

A simple POC Map Reduce Library for [IPFS](http://ipfs.io) in Golang

## Design

This acts as a standalone library. So only [IPFS](https://github.com/ipfs/go-ipfs) nodes using this library to register services will be able to communicate to each other to perform MapReduce.

In the future the plan it to have this part of the daemon so all IPFS nodes have this protocol and we can do p2p map reduce at a much larger scale. Because IPFS slits a file added to it into 256 KB blocks, we can independently process them. 

Every Peer (a IPFS node) registers the map & reduce [gorpc](https://github.com/libp2p/go-libp2p-gorpc) services using the library. This sets the required stream handlers for the [libp2p](https://github.com/libp2p/go-libp2p) protocol "/ipfs/mapreduce".

To run mapreduce, we use the library to get the master struct, registering the master service and initializing by passing in the required variables. Files and stored and fetched from IPFS via the [Cid](https://docs.ipfs.io/concepts/content-addressing/) indentifier.
- node: the ipfs node used to connect to peers, fetch files, etc.
- mapFuncFilePath: map golang code built to a ".so" file in plugin mode
- reduceFuncFilePath: reduce golang code built to a ".so" file in plugin mode
- noOfReducers: no of reducers
- dataFileCid: cid string for the data file to process using map reduce.

Calling a run method on the master starts the map reduce process.

## Usage

All Peers 
```go
import (
    ...
    mapreduce "github.com/omkarprabhu-98/go-ipfs-mapreduce"
    ...
)

fmt.Println("Spawning ephemeral ipfs node")
node, err := spawnEphemeral(ctx)
if err != nil {
    panic(fmt.Errorf("failed to spawn ephemeral node: %s", err))
}
err = mapreduce.RegisterProtocol(node)
if err != nil {
    panic(fmt.Errorf("failed to register map reduce protocol: %s", err))
}
```

Run Map Reduce
```go
master, err := mapreduce.InitMaster(node, mapFuncFilePath, reduceFuncFilePath, 
noOfReducers, dataFileCid);
if err != nil {
    panic(fmt.Errorf("failed to init master: %s", err))
}
master.RunMapReduce(ctx)
```

Observe status
```go
ticker := time.NewTicker(5 * time.Second)
quit := make(chan struct{})
go func() {
    for {
        select {
            case <- ticker.C:
                fmt.Println("MapStatus:", master.GetMapStatus())
                redStatus := master.GetReduceStatus()
                fmt.Println("ReduceStatus:", redStatus)
                if redStatus.Complete == redStatus.Total {
                    quit <- struct{}{}
                }
            case <- quit:
                ticker.Stop()
                return
        }
    }
}()
```

Check `examples` directory for examples 

## Demo

Snippets of sample runs locally

1. Small input file 1KB

https://user-images.githubusercontent.com/23053768/129325774-5017407f-edbf-4227-a362-26d0d3e4a241.mov

2. Large file 581 KB

https://user-images.githubusercontent.com/23053768/129325623-03e7be66-99ef-4534-9e8d-f7f9cd9d5b0e.mov


## References
Inspiration and code references are taken from MIT's 6.824 course
- http://nil.csail.mit.edu/6.824/2020/schedule.html
- https://github.com/WenbinZhu/mit-6.824-labs

## TODOs

- [ ] testing
- [ ] complete code TODOs
- [ ] handle timeouts and add retries
- [ ] integrate into go-ifs

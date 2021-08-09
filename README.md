# Go IPFS MapReduce

A simple POC Map Reduce Library for [IPFS](http://ipfs.io) in Golang

## Design

This acts as a standalone library. So only [IPFS](https://github.com/ipfs/go-ipfs) nodes using this library to register services will be able to communicate to each other to perform MapReduce.

In the future the plan it to have this part of the daemon so all IPFS nodes have this protocol and we can do p2p map reduce at a much larger scale.

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

```

Run Map Reduce
```go

```

Check `examples` directory for examples 

## Demo

Snippets of sample runs locally

1. Small input file

2. Large file


## References
Inspiration and code references are taken from MIT's 6.824 course
- http://nil.csail.mit.edu/6.824/2020/schedule.html
- https://github.com/WenbinZhu/mit-6.824-labs

## TODOs

- [ ] testing
- [ ] complete code TODOs
- [ ] handle timeouts and add retries
- [ ] integrate into go-ifs
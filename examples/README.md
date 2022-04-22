# Examples directory

## Running TF-IDF

1. On the peer nodes - which have shards
```
go run mr_run.go <input_file_to_be_sharded> <shard_cids_output_file> <no_of_lines_per_shard>
```
Eg. 
```
go run mr_run.go data.csv /sharedDir/o.t 1000
```

2. On the node which acts as the master
```
go run mr_run.go <no_of_reducers> <input_file_cid> <output_file> <shard_cids_input_file> <no_of_docs_in_dataset>
```
Where `<input_file_cid> ` is optional with `<shard_cids_input_file>`

Eg. 
```
go run mr_run.go 1 dummy output /sharedDir/o.t 1000000
```
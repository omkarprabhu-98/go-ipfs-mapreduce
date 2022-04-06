# Examples directory

## 1. Word Count

Normal Peer that first uploads the data file to IPFS

```
go run mr_run.go <data-file-path>
```

Run MR and observe status

```
go run mr_run.go <map-func-plugin-path> <reduce-func-plugin-path> <no-of-reducers> <data-file-cid>
```

## 2. TF IDF

### Round 1
Normal Peer that first uploads the data file to IPFS

```
go run mr_run.go tfidf/data.csv
```

Run MR and observe status

```
go run mr_run.go <round> <no-of-reducers> <file-cid> tfidf/out1.txt
```

### Round 2
Normal Peer that first uploads the data file to IPFS

```
go run mr_run.go tfidf/out1.txt
```

Run MR and observe status

```
go run mr_run.go <round> <no-of-reducers> <file-cid> tfidf/out2.txt
```

### Round 3
Normal Peer that first uploads the data file to IPFS

```
go run mr_run.go tfidf/out2.txt
```

Run MR and observe status

```
go run mr_run.go <round> <no-of-reducers> <file-cid> tfidf/ans.txt
```
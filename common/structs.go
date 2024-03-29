package common

type Empty struct {
	// Success bool
	// Error   error // else Error string
}

type MapInput struct {
	FuncFileCid  string
	NoOfReducers int
	DataFileCid  string 
	MasterPeerId string
}

type MapOutput struct {
	DataFileCid  string
	KvFileCids   []string
}

type KeyValue struct {
	Key   string
	Value string
}

type ReduceInput struct {
	FuncFileCid  string
	KvFileCids   []string
	MasterPeerId string
	ReducerNo    int
}

type ReduceOutput struct {
	ReducerNo     int
	OutputFileCid string
}
package common

const (
	ProtocolID = "/ipfs/mapreduce"
	MaxRetries = 3
	MaxGoRoutines = 250

	// Master
	NoOfProviders = 5
	MasterMapOutputFuncName = "ProcessMapOutput"
	MasterReduceOutputFuncName = "ProcessReduceOutput"
	MasterServiceName = "Master"

	// Mapper
	MapFuncName = "Map"
	MapServiceName = "MapService"

	// Reducer
	ReduceFuncName = "Reduce"
	ReduceServiceName = "ReduceService"
)
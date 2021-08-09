package common

const (
	ProtocolID = "/ipfs/mapreduce"

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
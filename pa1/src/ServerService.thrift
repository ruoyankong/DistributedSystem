include "Address.thrift"
service ServerService{
	bool addNode(1: Address.Address address),
	bool getSentVal(1: string inputDir),
	bool update(1: Address.Address address, 2: string taskFilename, 3: i64 duration, 4: string taskType)
	
}

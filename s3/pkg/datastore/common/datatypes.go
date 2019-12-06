package common

type PutResult struct {
	// bytes written to backend.
	Written int64
	// object id
	ObjectId string
	// object content hash sum string.
	Etag string
	// meta info for this storage driver.
	// only storage driver needs to care about this.
	// meta will be save by grpc server and transfer it to storage driver.
	Meta string
	// update time for this object.
	//UpdateTime time.Time
	UpdateTime int64
}

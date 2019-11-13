package types

import (
	"time"
)

const (
	MULTIPART_UPLOAD_IN_PROCESS = 0
	MULTIPART_UPLOAD_COMPLETE   = 1
)

type PartInfo struct {
	// global unique upload id
	// Note: this upload id is increasing monoatomicly only in one node.
	UploadId uint64
	// part number in this upload
	PartNum uint
	// object id in ceph which relates to the part.
	ObjectId string
	// ceph cluster
	Location string
	// ceph pool name
	Pool string
	// offset of this part in the whole object
	Offset uint64
	// size of this part
	Size uint64
	// etag of this part
	Etag string
	// Flag determin whether this part is completed or not.
	// 0 by default.
	Flag uint8
	// create time of this upload
	CreateTime time.Time
	// this record changed time.
	UpdateTime time.Time
}

type ByPartNum []*PartInfo

func (bpn ByPartNum) Len() int { return len(bpn) }

func (bpn ByPartNum) Swap(i, j int) { bpn[i], bpn[j] = bpn[j], bpn[i] }

func (bpn ByPartNum) Less(i, j int) bool { return bpn[i].PartNum <= bpn[j].PartNum }

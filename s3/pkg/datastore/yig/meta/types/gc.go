package types

import (
	"time"
)

type GcObject struct {
	Id         int64
	Location   string
	Pool       string
	ObjectId   string
	CreateTime time.Time
}
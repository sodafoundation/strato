package model

import (
	"github.com/globalsign/mgo/bson"
)

type Backend struct {
	Id         bson.ObjectId `json:"id,omitempty" bson:"_id,omitempty"`
	TenantId   string        `json:"tenantId,omitempty" bson:"tenantId,omitempty"`
	UserId     string        `json:"userId,omitempty" bson:"userId,omitempty"`
	Name       string        `json:"name,omitempty" bson:"name,omitempty"`
	Type       string        `json:"type,omitempty" bson:"type,omitempty"`
	Region     string        `json:"region,omitempty" bson:"region,omitempty"`
	Endpoint   string        `json:"endpoint,omitempty" bson:"endpoint,omitempty"`
	BucketName string        `json:"bucketName,omitempty" bson:"bucketName,omitempty"`
	Access     string        `json:"access,omitempty" bson:"access,omitempty"`
	Security   string        `json:"security,omitempty" bson:"security,omitempty"`
}

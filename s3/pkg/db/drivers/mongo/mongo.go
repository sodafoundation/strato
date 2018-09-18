package mongo

import (
	"github.com/globalsign/mgo"
)

var adap = &adapter{}
var DataBaseName = "metadatastore"
var BucketMD = "metadatabucket"

func Init(host string) *adapter {
	//fmt.Println("edps:", deps)
	session, err := mgo.Dial(host)
	if err != nil {
		panic(err)
	}
	//defer session.Close()

	session.SetMode(mgo.Monotonic, true)
	adap.s = session
	adap.userID = "unknown"

	return adap
}

func Exit() {
	adap.s.Close()
}

type adapter struct {
	s      *mgo.Session
	userID string
}

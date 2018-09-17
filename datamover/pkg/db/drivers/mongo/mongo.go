package mongo

import(
	"github.com/globalsign/mgo"
	"time"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/globalsign/mgo/bson"
	"errors"
	"github.com/micro/go-log"
)

var adap = &adapter{}

var DataBaseName = "test"
var CollJob = "job"

type MyLock struct{
	LockObj string `bson:"lockobj"`
	LockTime time.Time `bson:"locktime"`
}

func Init(host string) *adapter {
	//log.Log("edps:", deps)
	session,err := mgo.Dial(host)
	if err != nil{
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
	s *mgo.Session
	userID string
}

func (ad *adapter) UpdateJob(job *Job) error {
	ss := ad.s.Copy()
	defer ss.Close()

	c := ss.DB(DataBaseName).C(CollJob)
	j := Job{}
	err := c.Find(bson.M{"_id":job.Id}).One(&j)
	if err != nil{
		log.Logf("Get job failed before update it, err:%v\n", err)
		return errors.New("Get job failed before update it.")
	}

	if !job.StartTime.IsZero() {
		j.StartTime = job.StartTime
	}
	if !job.EndTime.IsZero() {
		j.EndTime = job.EndTime
	}
	if job.TotalCapacity != 0 {
		j.TotalCapacity = job.TotalCapacity
	}
	if job.TotalCount != 0 {
		j.TotalCount = job.TotalCount
	}
	if job.PassedCount != 0 {
		j.PassedCount = job.PassedCount
	}
	if job.PassedCapacity != 0 {
		j.PassedCapacity = job.PassedCapacity
	}
	if job.Status != "" {
		j.Status = job.Status
	}

	err = c.Update(bson.M{"_id":j.Id}, &j)
	if err != nil {
		log.Fatalf("Update job in database failed, err:%v\n", err)
		return errors.New("Update job in database failed.")
	}

	log.Logf("Update job in database successfully\n", err)
	return nil
}

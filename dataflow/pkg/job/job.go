package job

import (
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/opensds/go-panda/dataflow/pkg/db"
)

func Create(job *Job) error {
	jobId := bson.NewObjectId()
	job.Id = jobId

	err := db.DbAdapter.CreateJob(job)
	for i := 0; i < 3; i++ {
		if err == nil || err == ERR_DB_ERR {
			return err
		}
		//Otherwise err is ERR_DB_IDX_DUP
		jobId = bson.NewObjectId()
		job.Id = jobId
		err = db.DbAdapter.CreateJob(job)
	}

	log.Log("Add job failed, objectid duplicate too much times.")
	return ERR_INNER_ERR
}

func Get(id string, tenant string) ([]Job, error) {
	return db.DbAdapter.GetJob(id, tenant)
}

package job

import (
	"github.com/globalsign/mgo/bson"
	"fmt"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/opensds/go-panda/dataflow/pkg/db"
)

func Create(job *Job) ErrCode {
	jobId := bson.NewObjectId()
	job.Id = jobId

	err := db.DbAdapter.CreateJob(job)
	for i := 0; i < 3; i++ {
		if err == ERR_OK || err == ERR_DB_ERR {
			return err
		}
		//Otherwise err is ERR_DB_IDX_DUP
		jobId = bson.NewObjectId()
		job.Id = jobId
		err = db.DbAdapter.CreateJob(job)
	}

	fmt.Println("Add job failed, objectid duplicate too much times.")
	return ERR_INNER_ERR
}

func Get(id string, tenant string) ([]Job, ErrCode) {
	return db.DbAdapter.GetJob(id, tenant)
}

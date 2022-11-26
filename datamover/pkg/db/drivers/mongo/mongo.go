// Copyright 2019 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mongo

import (
	"context"
	"errors"

	"github.com/globalsign/mgo/bson"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	backend "github.com/opensds/multi-cloud/backend/pkg/model"
	. "github.com/opensds/multi-cloud/dataflow/pkg/model"
)

var adap = &adapter{}

var DataBaseName = "multi-cloud"
var CollJob = "job"
var CollBackend = "backends"
var mongodb = "mongodb://"

func Init(host string) *adapter {
	// Create a new client and connect to the server
	uri := mongodb + host
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		panic(err)
	}
	log.Debugln("Successfully connected and pinged.")

	adap.s = client
	adap.userID = "unknown"

	return adap
}

func Exit() {
	adap.s.Disconnect(context.TODO())
}

type adapter struct {
	s      *mongo.Client
	userID string
}

func (ad *adapter) GetJobStatus(jobID string) string {
	job := Job{}
	ss := ad.s
	c := ss.Database(DataBaseName).Collection(CollJob)

	err := c.FindOne(context.TODO(), bson.M{"_id": bson.ObjectIdHex(jobID)}).Decode(&job)
	if err != nil {
		log.Errorf("Get job[ID#%s] failed:%v.\n", jobID, err)
		return ""
	}

	return job.Status
}

func (ad *adapter) UpdateJob(job *Job) error {
	ss := ad.s

	c := ss.Database(DataBaseName).Collection(CollJob)
	j := Job{}
	err := c.FindOne(context.TODO(), bson.M{"_id": job.Id}).Decode(&j)
	if err != nil {
		log.Errorf("Get job[id:%v] failed before update it, err:%v\n", job.Id, err)

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
	if job.Progress != 0 {
		j.Progress = job.Progress
	}

	_, err = c.UpdateOne(context.TODO(), bson.M{"_id": j.Id}, bson.M{"$set": j})
	if err != nil {
		log.Errorf("Update job in database failed, err:%v\n", err)
		return errors.New("Update job in database failed.")
	}

	log.Info("Update job in database succeed.")
	return nil
}

func (ad *adapter) GetBackendByName(name string) (*backend.Backend, error) {
	log.Infof("Get backend by name:%s\n", name)
	session := ad.s
	var backend = &backend.Backend{}
	collection := session.Database(DataBaseName).Collection(CollBackend)
	err := collection.FindOne(context.TODO(), bson.M{"name": name}).Decode(backend)
	if err != nil {
		return nil, err
	}

	return backend, nil
}

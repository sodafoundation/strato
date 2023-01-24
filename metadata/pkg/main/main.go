/*
 * // Copyright 2023 The SODA Authors.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 *
 */

package main

import (
	"encoding/json"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {

	query := bson.D{
		bson.E{Key: "$match", Value: bson.D{
			bson.E{Key: "backendName", Value: "backendName"}}}}

	result := []*model.MetaBackend{}
	queryStr, err := json.Marshal(query)
	if err != nil {
		log.Errorln("Failed to marshal query")
	}

	resultStr, err := json.Marshal(result)
	if err != nil {
		log.Errorln("Failed to marshal result")
	}
	log.Infoln("result:", resultStr)
	log.Infoln("query:", queryStr)

}

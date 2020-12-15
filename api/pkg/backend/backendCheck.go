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


package backend

import (
        "github.com/opensds/multi-cloud/backend/proto"
	"context"
)

func backendCheck(ctx context.Context ,backendDetail  *backend.BackendDetail)(error){

	if (backendDetail.Type=="azure-blob"){
		err:= azureCheck(ctx,backendDetail)
		return err
	}

	 if (backendDetail.Type=="aws-s3"){
                err:= awsCheck(backendDetail)
                return err
        }



	return nil
}


#!/bin/bash

# Copyright 2021 The SODA Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "Executing initialize-mongo-rs.sh" >> initialize-mongo-rs-log.txt
mongo --eval "mongodb = ['$MONGODB_0_SERVICE_SERVICE_HOST:$MONGODB_0_SERVICE_SERVICE_PORT', '$MONGODB_1_SERVICE_SERVICE_HOST:$MONGODB_1_SERVICE_SERVICE_PORT', '$MONGODB_2_SERVICE_SERVICE_HOST:$MONGODB_2_SERVICE_SERVICE_PORT']" --shell << EOL
cfg = {
        _id: "rs0",
        members:
            [
                {_id : 0, host : mongodb[0], priority : 1},
                {_id : 1, host : mongodb[1], priority : 0.9},
                {_id : 2, host : mongodb[2], priority : 0.5}
            ]
        }
rs.initiate(cfg)
EOL
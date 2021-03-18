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

mkdir /data/db/rs0-0

if [[ $MONGO_CONFIGURE_REPLICA_SET == true ]]
then
    /root/reconfig-mongo-rs.sh &

    export POD_IP_ADDRESS=$(hostname -i)
    mongod --replSet rs0 --port 27017 --bind_ip localhost,$POD_IP_ADDRESS --dbpath /data/db/rs0-0 --oplogSize 128

    echo "POD_IP_ADDRESS: $POD_IP_ADDRESS:$MONGODB_0_SERVICE_SERVICE_PORT" >> /root/env.txt
    echo "MONGODB_0_SERVICE_SERVICE_HOST: $MONGODB_0_SERVICE_SERVICE_HOST:$MONGODB_0_SERVICE_SERVICE_PORT" >> /root/env.txt
    echo "MONGODB_1_SERVICE_SERVICE_HOST: $MONGODB_1_SERVICE_SERVICE_HOST:$MONGODB_1_SERVICE_SERVICE_PORT" >> /root/env.txt
    echo "MONGODB_2_SERVICE_SERVICE_HOST: $MONGODB_2_SERVICE_SERVICE_HOST:$MONGODB_2_SERVICE_SERVICE_PORT" >> /root/env.txt
else
    echo "Starting MongoDB as standalone instance" >> reconfig-mongo-rs-log.txt
    mongod --port 27017 --bind_ip localhost,$POD_IP_ADDRESS,$MONGODB_0_SERVICE_SERVICE_HOST --dbpath /data/db/rs0-0
fi
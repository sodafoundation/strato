#!/bin/bash

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
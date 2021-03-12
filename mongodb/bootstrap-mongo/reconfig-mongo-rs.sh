#!/bin/bash

retryCount=0

echo "Checking MongoDB status:"

while [[ $(mongo --quiet --eval "rs.conf()._id") != rs0 ]]
do
    if [ $retryCount -gt 30 ]
    then
        echo "Retry count > 30, breaking out of while loop now..." >> reconfig-mongo-rs-log.txt
        break
    fi
    echo "MongoDB not ready for Replica Set configuration, retrying in 5 seconds..." >> reconfig-mongo-rs-log.txt
    sleep 5
    retryCount=$((retryCount+1))
done

echo "Sending in Replica Set configuration..." >> reconfig-mongo-rs-log.txt

mongo --eval "mongodb = ['$MONGODB_0_SERVICE_SERVICE_HOST:$MONGODB_0_SERVICE_SERVICE_PORT', '$MONGODB_1_SERVICE_SERVICE_HOST:$MONGODB_1_SERVICE_SERVICE_PORT', '$MONGODB_2_SERVICE_SERVICE_HOST:$MONGODB_2_SERVICE_SERVICE_PORT']" --shell << EOL
cfg = rs.conf()
cfg.members[0].host = mongodb[0]
cfg.members[1].host = mongodb[1]
cfg.members[2].host = mongodb[2]
rs.reconfig(cfg, {force: true})
EOL

sleep 5

if [[ $(mongo --quiet --eval "db.isMaster().setName") != rs0 ]]
then
    echo "Replica Set reconfiguratoin failed..." >> reconfig-mongo-rs-log.txt
    echo "Reinitializing Replica Set..." >> reconfig-mongo-rs-log.txt
    /root/initialize-mongo-rs.sh &
else
    echo "Replica Set reconfiguratoin successful..." >> reconfig-mongo-rs-log.txt
fi
#!/bin/bash

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
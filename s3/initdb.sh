#!/bin/sh
#host=$(ip route show | grep "default" | awk '{print $3}' | xargs echo -n)
#echo $host
succeed=false
for i in 1 2 3 4 5
do
    echo  "trying again, attempt " $i
    mysql -h tidb -P 4000 -u root -e "create database if not exists s3 character set utf8;use s3;source /etc/config/tidb/tidb.sql;create database if not exists yig character set utf8;use yig;source /etc/config/yig/yig.sql;"
    if [ "$?" = "0" ]; then
        succeed=true
        break
    fi
    echo  "waiting 2 seconds, to try again"
    sleep 2
done
if [ "${succeed}" = "true" ]; then
    echo "create database succeeded..."
else
    echo "create database failed..."
fi
./s3

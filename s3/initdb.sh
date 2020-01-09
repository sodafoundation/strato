#!/bin/sh
#host=$(ip route show | grep "default" | awk '{print $3}' | xargs echo -n)
#echo $host
succeed=false
for i in 1 2 3 4 5
do
    echo  "trying again, attempt " $i
    mysql -h tidb -P 4000 -u root -e "create database if not exists s3 character set utf8;use s3;source ./s3.sql;"
    if [ "$?" = "0" ]; then
        succeed=true
        break
    fi
    sleep 5
    echo  "sleeping to try again"
done
if [ "${succeed}" = "true" ]; then
    echo "create database succeed ..."
else
    echo "create database failed ..."
fi
./s3

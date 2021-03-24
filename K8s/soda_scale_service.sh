#!/bin/sh
no_of_pods="$1"
service_name="$2"

if [ -z "$service_name" ] || [ -z "$no_of_pods" ]
then
        echo "Usage : ./soda_scale_service <service_name> <number_of_instances>"
else
        echo "START :: Scaling Service " $2 to $1 "instances.."
        cmd="kubectl scale --replicas=$2  deployment/$1 -n soda-multi-cloud"
        echo "EXECUTING ... $cmd"
        bash -c "$cmd"
        echo "COMPLETED :: Scaling Service " $2 to $1 "instances..."
fi

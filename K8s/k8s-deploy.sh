# Run this file from multi-cloud folder.
kubectl apply -f rbac.yaml -n soda-multi-cloud

#1. Create all the config Maps from files. 
kubectl create configmap multicloud-config --from-file=../examples/multi-cloud.conf  -n soda-multi-cloud
kubectl create configmap tidb-config --from-file=../s3/tidbconf/tidb.toml -n soda-multi-cloud
kubectl create configmap s3-config --from-file=../s3/pkg/conf/s3.toml -n soda-multi-cloud
kubectl create configmap tidb-sql --from-file=../s3/pkg/conf/tidb.sql -n soda-multi-cloud
kubectl create configmap s3-sql --from-file=../s3/pkg/conf/tidb.sql -n soda-multi-cloud
kubectl create configmap yig-sql --from-file=../s3/pkg/datastore/yig/conf/yig.sql -n soda-multi-cloud

#2. Create all pods with no dependencies. 
kubectl apply -f zookeeper-deployment.yaml -n soda-multi-cloud
kubectl apply -f redis-deployment.yaml -n soda-multi-cloud
kubectl apply -f tidb-deployment.yaml -n soda-multi-cloud
kubectl apply -f kafka-deployment.yaml -n soda-multi-cloud

kubectl apply -f block-deployment.yaml -n soda-multi-cloud
kubectl apply -f file-deployment.yaml -n soda-multi-cloud
kubectl apply -f backend-deployment.yaml -n soda-multi-cloud
kubectl apply -f s3api-deployment.yaml -n soda-multi-cloud
kubectl apply -f api-deployment.yaml -n soda-multi-cloud
kubectl apply -f datamover-deployment.yaml -n soda-multi-cloud
kubectl apply -f dataflow-deployment.yaml -n soda-multi-cloud
kubectl apply -f s3-deployment.yaml -n soda-multi-cloud

#4 Expose the services
kubectl apply -f zookeeper-service.yaml -n soda-multi-cloud
kubectl apply -f tidb-service.yaml -n soda-multi-cloud
kubectl apply -f redis-service.yaml -n soda-multi-cloud
kubectl apply -f kafka-service.yaml -n soda-multi-cloud
kubectl apply -f api-service.yaml -n soda-multi-cloud
kubectl apply -f s3api-service.yaml -n soda-multi-cloud

#5. Get all the deployed objects
kubectl get all -n soda-multi-cloud

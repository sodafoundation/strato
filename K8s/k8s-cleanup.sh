#Delete the services
kubectl delete service  zookeeper -n soda-multi-cloud
kubectl delete service  tidb -n soda-multi-cloud
kubectl delete service  redis -n soda-multi-cloud
kubectl delete service  kafka -n soda-multi-cloud
kubectl delete service  api -n soda-multi-cloud
kubectl delete service  s3api -n soda-multi-cloud
kubectl delete service  mongodb-0-service -n soda-multi-cloud
kubectl delete service  mongodb-1-service -n soda-multi-cloud
kubectl delete service  mongodb-2-service -n soda-multi-cloud

# Delete the deployments
kubectl delete deployment zookeeper -n soda-multi-cloud
kubectl delete deployment redis -n soda-multi-cloud
kubectl delete deployment tidb -n soda-multi-cloud
kubectl delete deployment kafka-svc -n soda-multi-cloud
kubectl delete deployment block -n soda-multi-cloud
kubectl delete deployment file -n soda-multi-cloud
kubectl delete deployment backend -n soda-multi-cloud
kubectl delete deployment s3api -n soda-multi-cloud
kubectl delete deployment s3 -n soda-multi-cloud
kubectl delete deployment api -n soda-multi-cloud
kubectl delete deployment datamover -n soda-multi-cloud
kubectl delete deployment dataflow -n soda-multi-cloud
kubectl delete deployment  mongodb-0 -n soda-multi-cloud
kubectl delete deployment  mongodb-1 -n soda-multi-cloud
kubectl delete deployment  mongodb-2 -n soda-multi-cloud

# Delete config maps
kubectl delete configmap multicloud-config -n soda-multi-cloud
kubectl delete configmap tidb-config -n soda-multi-cloud
kubectl delete configmap s3-config -n soda-multi-cloud
kubectl delete configmap tidb-sql -n soda-multi-cloud
kubectl delete configmap s3-sql -n soda-multi-cloud
kubectl delete configmap yig-sql -n soda-multi-cloud

# Delete pv, pvc
kubectl patch pvc mongo-0-pv-claim -p '{"metadata":{"finalizers": []}}' --type=merge -n soda-multi-cloud
kubectl patch pvc mongo-1-pv-claim -p '{"metadata":{"finalizers": []}}' --type=merge -n soda-multi-cloud
kubectl patch pvc mongo-2-pv-claim -p '{"metadata":{"finalizers": []}}' --type=merge -n soda-multi-cloud
 
kubectl delete pvc mongo-0-pv-claim -n soda-multi-cloud
kubectl delete pvc mongo-1-pv-claim -n soda-multi-cloud
kubectl delete pvc mongo-2-pv-claim -n soda-multi-cloud

kubectl delete pv mongo-0-pv-volume -n soda-multi-cloud
kubectl delete pv mongo-1-pv-volume -n soda-multi-cloud
kubectl delete pv mongo-2-pv-volume -n soda-multi-cloud
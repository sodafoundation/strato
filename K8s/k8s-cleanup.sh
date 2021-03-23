#Delete the services
kubectl delete service  zookeeper -n soda-multi-cloud
kubectl delete service  tidb -n soda-multi-cloud
kubectl delete service  redis -n soda-multi-cloud
kubectl delete service  kafka -n soda-multi-cloud
kubectl delete service  api -n soda-multi-cloud
kubectl delete service  s3api -n soda-multi-cloud
kubectl delete service  s3 -n soda-multi-cloud
kubectl delete service  backend -n soda-multi-cloud
kubectl delete service  block -n soda-multi-cloud
kubectl delete service  file -n soda-multi-cloud
kubectl delete service  datamover -n soda-multi-cloud
kubectl delete service  dataflow -n soda-multi-cloud

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

# Delete config maps
kubectl delete configmap multicloud-config -n soda-multi-cloud
kubectl delete configmap tidb-config -n soda-multi-cloud
kubectl delete configmap s3-config -n soda-multi-cloud
kubectl delete configmap tidb-sql -n soda-multi-cloud
kubectl delete configmap s3-sql -n soda-multi-cloud
kubectl delete configmap yig-sql -n soda-multi-cloud

#Cleanup Configmaps
kubectl delete configmap multicloud-config -n soda-multi-cloud
kubectl delete configmap tidb-config -n soda-multi-cloud
kubectl delete configmap s3-config -n soda-multi-cloud
kubectl delete configmap tidb-sql -n soda-multi-cloud
kubectl delete configmap s3-sql -n soda-multi-cloud
kubectl delete configmap yig-sql -n soda-multi-cloud


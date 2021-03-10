#Delete the services
kubectl delete service  zookeeper 
kubectl delete service  tidb 
kubectl delete service  redis 
#kubectl delete service  datastore 
kubectl delete service  kafka 
		
kubectl delete service  api 
kubectl delete service  s3api 
kubectl delete service  s3 
kubectl delete service  backend 
kubectl delete service  block 
kubectl delete service  file 
kubectl delete service  datamover 
kubectl delete service  dataflow 
kubectl delete service  mongo

kubectl delete statefulset mongo 

# Delete the deployments

kubectl delete deployment zookeeper 
kubectl delete deployment redis 
kubectl delete deployment tidb 
kubectl delete deployment datastore 
kubectl delete deployment kafka-svc 
			   
kubectl delete deployment block 
kubectl delete deployment file 
kubectl delete deployment backend 
kubectl delete deployment s3api 
kubectl delete deployment s3 
kubectl delete deployment api 
kubectl delete deployment datamover 
kubectl delete deployment dataflow 

kubectl delete configmap multicloud-config 
kubectl delete configmap tidb-config 
kubectl delete configmap s3-config  
 
kubectl delete configmap tidb-sql 
kubectl delete configmap s3-sql 
kubectl delete configmap yig-sql 


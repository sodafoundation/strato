kubectl apply -f namespace.yaml

kubectl apply -f mongo-pv-0.yaml -n soda-multi-cloud
kubectl apply -f mongo-pv-1.yaml -n soda-multi-cloud
kubectl apply -f mongo-pv-2.yaml -n soda-multi-cloud
 
kubectl apply -f mongo-pvc-0.yaml -n soda-multi-cloud
kubectl apply -f mongo-pvc-1.yaml -n soda-multi-cloud
kubectl apply -f mongo-pvc-2.yaml -n soda-multi-cloud

kubectl apply -f mongo-service-0.yaml -n soda-multi-cloud
kubectl apply -f mongo-service-1.yaml -n soda-multi-cloud
kubectl apply -f mongo-service-2.yaml -n soda-multi-cloud



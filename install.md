

https://airflow.apache.org/docs/helm-chart/stable/index.html


helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace


kubectl get svc -n airflow

minikube service -n airflow airflow-webserver

kullanici olustur
kubectl exec -it -n airflow deploy/airflow-webserver -- bash

# Custom container ı olustur
docker buildx build --platform linux/amd64,linux/arm64 \
  -t innovai/airflow-custom:latest \
  --push \
  --provenance=false .


airflow users create \
  --username airflow \
  --firstname Air \
  --lastname Flow \
  --role Admin \
  --email airflow@example.com \
  --password airflow123


# Postgresql kurulum

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
kubectl create namespace medscan
helm install medscan-postgres \
  -n medscan \
  -f values.yaml \
  bitnami/postgresql


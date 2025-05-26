

https://airflow.apache.org/docs/helm-chart/stable/index.html


helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace


kubectl get svc -n airflow

minikube service -n airflow airflow-webserver

kullanici olustur
kubectl exec -it -n airflow deploy/airflow-webserver -- bash

# https://hub.docker.com/repositories/innovai

# Custom container ı olustur
docker buildx build --platform linux/amd64,linux/arm64 \
  -t innovai/airflow-custom:latest \
  --push \
  --provenance=false .

# multi platform image olusturmak icin
docker buildx build --platform linux/amd64,linux/arm64 \
  -t innovai/airflow-custom:latest \
  --push .

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


# airflow değişken postgresql


*	
medscan_postgres_conn

{
    "user": "medscan_user",
    "password": "medscan_pass",
    "host": "medscan-postgres-postgresql.medscan.svc.cluster.local",
    "port": "5432",
    "database": "dataml"
}


# Airflow variables
DATAML_DB_HOST=medscan-postgres-postgresql.medscan.svc.cluster.local
DATAML_DB_PORT=5432
DATAML_DB_NAME=dataml
DATAML_DB_USER=medscan_user
DATAML_DB_PASSWORD=medscan_pass




# Ortam geliştirme kurulumu
conda init bash # Terminali kapa aç
conda env create -f environment.yml
conda activate medscan

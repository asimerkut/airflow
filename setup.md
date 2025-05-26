helm repo add apache-airflow https://airflow.apache.org

helm upgrade --install airflow apache-airflow/airflow \
  --version 1.16.0 \
  --namespace airflow \
  --create-namespace \
  -f values.yaml

kubectl exec -it -n airflow airflow-webserver-5bb6d7b8c9-n9kgg -- /bin/bash


airflow users create \
  --username airflow \
  --firstname Air \
  --lastname Flow \
  --role Admin \
  --email airflow@example.com \
  --password airflow123





### Ollama Kurulumu

ollama run gemma3:27b

# Airflow Admin Variables
DATAML_DB_HOST	        host.docker.internal
DATAML_DB_NAME          dataml
DATAML_DB_PASSWORD      postgres
DATAML_DB_PORT          5432
DATAML_DB_USER          postgres
OLLAMA_BASE_URL         http://host.docker.internal:11434
OLLAMA_MODEL            gemma3:27b



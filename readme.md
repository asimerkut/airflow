https://airflow.apache.org/docs/helm-chart/stable/index.html


helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace


kubectl get svc -n airflow


minikube service -n airflow airflow-webserver


kullanici olustur
kubectl exec -it -n airflow deploy/airflow-webserver -- bash


# Custom container ı olustur
docker build -t innovai/airflow-custom:latest .
docker push innovai/airflow-custom:latest



airflow users create \
  --username airflow \
  --firstname Air \
  --lastname Flow \
  --role Admin \
  --email airflow@example.com \
  --password airflow123



helm upgrade airflow apache-airflow/airflow \
  -n airflow -f values.yaml
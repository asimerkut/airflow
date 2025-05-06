https://airflow.apache.org/docs/helm-chart/stable/index.html


helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace


kubectl get svc -n airflow


minikube service -n airflow airflow-webserver


kullanici olustur
kubectl exec -it -n airflow deploy/airflow-webserver -- bash



airflow users create \
  --username airflow \
  --firstname Air \
  --lastname Flow \
  --role Admin \
  --email airflow@example.com \
  --password airflow123


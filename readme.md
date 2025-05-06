minikube stop
minikube start

kubectl get svc -n airflow
kubectl get pods --all-namespaces


# minicube ile airflow start --> http://127.0.0.1:49823/home gibi bir adres verir
minikube service -n airflow airflow-webserver

# kodda değişiklik yapılır sa - values.yaml vs.
helm upgrade airflow apache-airflow/airflow \
  -n airflow -f values.yaml






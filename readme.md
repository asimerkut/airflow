minikube stop
minikube start

# To install fist time
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace



kubectl get svc -n airflow
kubectl get pods --all-namespaces


# minicube ile airflow start --> http://127.0.0.1:49823/home gibi bir adres verir
minikube service -n airflow airflow-webserver

# kodda değişiklik yapılır sa - values.yaml vs.
helm upgrade airflow apache-airflow/airflow \
  -n airflow -f values.yaml

# İttermeli update 
helm upgrade airflow apache-airflow/airflow -n airflow -f values.yaml --force


helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f values.yaml







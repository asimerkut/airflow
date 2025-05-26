minikube stop
minikube start --cpus=6 --memory=12288


# To install first time

helm repo add apache-airflow https://airflow.apache.org

helm upgrade --install airflow apache-airflow/airflow \
  --version 1.16.0 \
  --namespace airflow \
  --create-namespace \
  -f values.yaml





##### 
kubectl get svc -n airflow
kubectl get pods --all-namespaces

ollama kurululacak
şu paketler : gemma3, biobert nomic vs..



# minicube ile airflow start --> http://127.0.0.1:49823/home gibi bir adres verir
minikube service -n airflow airflow-webserver

# kodda değişiklik yapılır sa - values.yaml vs.
helm upgrade airflow apache-airflow/airflow \
  -n airflow -f values.yaml

# ittirmeli update 
helm upgrade airflow apache-airflow/airflow -n airflow -f values.yaml --force


helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f values.yaml

# kubernetes içinden connection testi, terminal>
minikube ssh
nc -vz host.minikube.internal 5432
nc -vz host.docker.internal 5432

# airflow versiyonu v2.10.5 olmali
pip show apache-airflow






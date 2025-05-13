FROM apache/airflow:2.10.5-python3.12

USER airflow

# Copy DAGs requirements file
COPY dags/requirements.txt /opt/airflow/dags/requirements.txt

# Install DAG requirements
RUN pip install -r /opt/airflow/dags/requirements.txt

# Copy DAGs directory
COPY dags/ /opt/airflow/dags/
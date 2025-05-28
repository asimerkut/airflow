FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update
RUN apt-get install -y mc
RUN apt-get install -y nano
RUN apt-get install -y libgomp1
RUN apt-get install -y openjdk-17-jdk

RUN mkdir -p /app && chmod -R 777 /app
COPY app/ /app/

USER airflow

# Copy DAGs requirements file
COPY dags/requirements.txt /opt/airflow/requirements.txt

# Install DAG requirements
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy DAGs directory directly to /opt/airflow/dags/
COPY dags/ /opt/airflow/dags/

# Verify installation
RUN pip list | grep langchain
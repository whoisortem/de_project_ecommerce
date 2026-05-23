FROM apache/airflow:2.9.1-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN curl -sSL \
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.10.txt" \
    -o /tmp/constraints.txt

RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark==4.7.2" \
    "kaggle" \
    --constraint /tmp/constraints.txt

FROM apache/airflow

USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow
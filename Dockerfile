FROM bitnami/airflow:2.10.2

USER root
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get remove libaom3 -y && \
    apt-get install -y git nano vim gcc
COPY requirements.txt requirements.txt
USER airflow
RUN pip install -r requirements.txt
RUN rm -f requirements.txt

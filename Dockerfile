FROM apache/airflow:2.7.2-python3.11

USER root
RUN apt-get update && \
    apt-get install -y git nano vim 
COPY requirements.txt requirements.txt
USER airflow
RUN pip install -r requirements.txt
RUN rm -f requirements.txt

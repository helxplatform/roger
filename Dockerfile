FROM apache/airflow:slim-2.10.2rc1-python3.12

USER root
RUN apt-get update && \
    apt-get install -y git nano vim 
COPY requirements.txt requirements.txt
USER airflow
RUN pip install -r requirements.txt
RUN rm -f requirements.txt

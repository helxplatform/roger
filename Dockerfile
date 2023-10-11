FROM apache/airflow:slim-2.7.1-python3.11
USER root
RUN apt-get update && \
    apt-get install -y git gcc python3-dev nano vim
COPY requirements.txt requirements.txt
USER airflow
RUN pip install -r requirements.txt
RUN rm -f requirements.txt

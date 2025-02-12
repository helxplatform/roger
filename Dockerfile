FROM bitnami/airflow:2.10.4

USER root
RUN apt-get update &&  apt-get install -y git nano vim gcc rustc cargo
#RUN useradd -u 1001 -ms /bin/bash airflow && chown -R airflow /home/airflow
COPY requirements.txt requirements.txt
RUN source /opt/bitnami/airflow/venv/bin/activate && CARGO_HOME=/tmp/.cargo pip install -r requirements.txt
RUN rm -f requirements.txt
#USER 1001

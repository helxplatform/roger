# Use a Debian-based image for better compatibility
FROM python:3.13-slim-trixie

# Set Airflow version and home directory
ARG AIRFLOW_VERSION=3.1.5
ARG AIRFLOW_HOME=/opt/airflow

# Environment variables
ENV AIRFLOW_HOME=${AIRFLOW_HOME}
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
ENV PYTHONUNBUFFERED=1

# Create airflow user and directories
RUN useradd --uid 50000 --home-dir ${AIRFLOW_HOME} --create-home airflow && \
    mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins ${AIRFLOW_HOME}/config

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    curl \
    tini \
    tzdata \
    git \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip tools
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install Airflow (with PostgreSQL, Celery, Redis support)
RUN pip install --no-cache-dir \
    "apache-airflow[postgres,celery,redis,fab]==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-cncf-kubernetes" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

# Optional: install extra packages
RUN pip install --no-cache-dir psycopg2-binary redis

COPY ./requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

RUN rm /tmp/requirements.txt

COPY . /opt/roger
RUN pip install -e /opt/roger

RUN apt-get purge -y --auto-remove \
    build-essential \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    curl \
    git && \
    apt-get clean

RUN if [ -n "$ROGER_SOURCE" ]; then pip install -e $ROGER_SOURCE; fi

# Set ownership
RUN chown -R airflow:airflow ${AIRFLOW_HOME}

# Switch to airflow user
USER airflow
WORKDIR ${AIRFLOW_HOME}

# Expose Airflow webserver port
EXPOSE 8080

# Use tini for signal handling
ENTRYPOINT ["/usr/bin/tini", "--"]

# Default command
CMD ["airflow", "webserver"]

# Merge Dug Architecture

## Introduction to Dug Architecture

Dug in the RTI Merge environment is a fork of the Helxplatform/Dug product but in an updated tech stack with adaptable architecture for new data source projects. Deployying Dug inside of Merge offers greater security and resiliency for the application. Dug is both the generic name for the product and a specific repository/application within the greater Dug environment. The architecture of Dug consists of four containers and one repository that uses CLI prompts to orchestrate the creation of the Dug data. The 5 components are described below.

- Roger - a repository with ETL pipeline python scripts to ochestrate [Dug](https://github.com/RTIInternational/dug) modules to extract and transform data with API calls via external services and then populate the Redisgraph and Elasticsearch databases.
  - The steps to perform the ETL pipeline can be found in the `./roger-cli-steps.md`
  - `./bin/Makefile all`
- Dug-API - Dug is both a python script repository used by Roger to perform ETL and a Python Flask API service that interacts with Elasticsearch to serve research queiries or a front end applocation. The API is created by running docker container with a *gunicorn* server as the gateway.
  - Available at port 5551 or 80:5551 as an API service inside Merge
  - Image built using the `./Dockerfile` in `main` branch in the Dug-Api repository <https://github.com/RTIInternational/Dug-Api>
- RedisGraph - Graph database based off of the Redis-Stack docker image with the RedisGraph module initalized via command args on build.
  - Available at port 6379 with the hostname `redis`
  - Docker Image redis/redis-stack:6.2.4-v2
  - *Will be converted to the Bitnami Redis Helm chart at production*
- Elasticsearch - NoSQL database where the finalized data of the Dug ETL pipeline is stored.
  - Exists at port 9200 and 9300
  - Docker Image docker.elastic.co/elasticsearch/elasticsearch:8.5.2
- Tranql - This is a graph database interpreter that exposes at Flask API for the RedisGraph database. This will most likely be replaced in the coming months.
  - Available at port 8001:8001/tranql inside the local build and on port 80:8001/tranql inside merge
  - Image built using the `./Dockerfile` in `develop` branch in the Dug repository <https://github.com/RTIInternational/tranql>

## RENCI Dug Deployment

The backend of Dug is all performed in the [RENCI Search Chart repo](https://github.com/helxplatform/search-chart) and can be used as an example of how they handle the production deployment of their system.

## Launching Dug

For a local deployment follow the steps outlined in the `./roger-cli-steps.md` these are summarized below.

1) Stand up all four docker containers: dug, tranql, redis, and elasticsearch
2) Create and activate a virtual python environment and then run `pip install -r requirments.txt`
3) Export all needed environment variables with the following syntax `ROGER_S3_` in the front of the value such as: `ROGER_S3_ACCESS__KEY=XXXXKEYXXXX`
4) Export the working directory path `export PYTHONPATH=$PWD/dags`
5) Inside the `./bin` directory run the command `make all`

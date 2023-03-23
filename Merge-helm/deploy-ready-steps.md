# Dug Backend Deployment Guide

## Dependencies

### ElasticSearch

### Redis Stack -- Redis Graph

### Redis Stack -- Redis

### Tranql

#### Values

existingRedis: Allows Tranql to use the a redis outside its own container.
``` yaml
existingRedis:
    host: ""
    port: 6379
    secret: search-redis-secret
    secretPasswordKey: password
```

### Dug - API

## Orchestration Requirements

### Environment Variables

#### Roger

Roger needs several env variables that are created by Helm Templates reading config.yaml. The ENVs that are required are:

- ROGER_S3_ACCESS__KEY: string -- RENCI S3 bucket access key
- ROGER_S3_BUCKET: string -- RENCI S3 bucket bucket name
- ROGER_S3_HOST: string -- RENCI S3 bucket host name
- ROGER_S3_SECRET__KEY: string -- RENCI S3 bucket secret key
- ROGER_DUG__INPUTS_DATA__SETS: string list: set to "bdc,nida,sparc,topmed,sprint,bacpac"
- ROGER_KGX_DATA__SETS: string list -- set to "baseline-graph,cde-graph"
- ROGER_INDEXING_NODE__TO__ELEMENT__QUERIES_ENABLED: boolean value -- set to true

There are other environment variables that can override the values given in `dags/roger/config/config.yaml`

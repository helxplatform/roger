{{/*
Expand the name of the chart.
*/}}
{{- define "search.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "search.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "search.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "search.labels" -}}
helm.sh/chart: {{ include "search.chart" . }}
{{ include "search.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "search.selectorLabels" -}}
app.kubernetes.io/name: {{ include "search.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "search.elasticsearch.fullname" -}}
{{- $name := default "elasticsearch" .Values.elasticsearch.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "search.elasticsearch.uname" -}}
{{- if empty .Values.elasticsearch.fullnameOverride -}}
{{- if empty .Values.elasticsearch.nameOverride -}}
{{ .Values.elasticsearch.clusterName }}-{{ .Values.elasticsearch.nodeGroup }}
{{- else -}}
{{ .Values.elasticsearch.nameOverride }}-{{ .Values.elasticsearch.nodeGroup }}
{{- end -}}
{{- else -}}
{{ .Values.elasticsearch.fullnameOverride }}
{{- end -}}
{{- end -}}

{{- define "search.elasticsearch.masterService" -}}
{{- if empty .Values.elasticsearch.masterService -}}
{{- if empty .Values.elasticsearch.fullnameOverride -}}
{{- if empty .Values.elasticsearch.nameOverride -}}
{{ .Values.elasticsearch.clusterName }}-master
{{- else -}}
{{ .Values.elasticsearch.nameOverride }}-master
{{- end -}}
{{- else -}}
{{ .Values.elasticsearch.fullnameOverride }}
{{- end -}}
{{- else -}}
{{ .Values.elasticsearch.masterService }}
{{- end -}}
{{- end -}}

{{/*
Get the elasticsearch password secret.
*/}}
{{- define "search.elasticsearch.secretName" -}}
{{- if .Values.elasticsearch.existingSecret -}}
{{- printf "%s" .Values.elasticsearch.existingSecret -}}
{{- else -}}
{{- printf "%s" (include "search.elasticsearch.fullname" .) -}}
{{- end -}}
{{- end -}}


{{/*
Get the username key to be retrieved from elasticsearch secret.
*/}}
{{- define "search.elasticsearch.secretUsernameKey" -}}
{{- if and .Values.elasticsearch.existingSecret .Values.elasticsearch.existingSecretUsernameKey -}}
{{- printf "%s" .Values.elasticsearch.existingSecretUsernameKey -}}
{{- else -}}
{{- printf "elasticsearch-username" -}}
{{- end -}}
{{- end -}}

{{/*
Get the password key to be retrieved from elasticsearch secret.
*/}}
{{- define "search.elasticsearch.secretPasswordKey" -}}
{{- if and .Values.elasticsearch.existingSecret .Values.elasticsearch.existingSecretPasswordKey -}}
{{- printf "%s" .Values.elasticsearch.existingSecretPasswordKey -}}
{{- else -}}
{{- printf "elasticsearch-password" -}}
{{- end -}}
{{- end -}}

{{/*
Expand the name of the chart.
*/}}
{{- define "search.api.name" -}}
{{- default "api" .Values.api.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name for web.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "search.api.web.fullname" -}}
{{- if .Values.api.fullnameOverride }}
{{- .Values.api.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default "api" .Values.api.nameOverride }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "search.api.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Environment vars for API pods
*/}}
{{- define "search.api.web.deploymentEnv" -}}
{{- if .Values.elasticsearch.enabled -}}
- name: ELASTIC_API_HOST
  value: {{ include "search.api.elasticsearch.uname" . }}
{{- end }}
{{- if .Values.redis.enabled }}
- name: REDIS_HOST
  value: {{ include "search.redis.fullname" . }}-master
- name: REDIS_PORT
  value: {{ .Values.redis.redisPort | quote }}
{{- if .Values.redis.usePassword }}
- name: REDIS_PASSWORD
  {{- if .Values.redis.password }}
  value: {{ .Values.redis.password }}
  {{- else }}
  valueFrom:
    secretKeyRef:
      name: {{ include "search.redis.secretName" . }}
      key: {{ include "search.redis.secretPasswordKey" . }}
  {{- end }}
  {{- end }}
  {{- end }}
  {{- if .Values.api.deployment.extraEnv }}
  {{- toYaml .Values.api.deployment.extraEnv | nindent 10 }}
  {{- end }}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "search.api.labels" -}}
helm.sh/chart: {{ include "search.api.chart" . }}
{{ include "search.api.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "search.api.selectorLabels" -}}
app.kubernetes.io/name: {{ include "search.api.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}


{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "search.api.elasticsearch.fullname" -}}
{{- $name := default "elasticsearch" .Values.elasticsearch.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
This is the name of the services that get created, e.g. elasticsearch-master
*/}}
{{- define "search.api.elasticsearch.uname" -}}
{{- if empty .Values.elasticsearch.fullnameOverride -}}
{{- if empty .Values.elasticsearch.nameOverride -}}
{{ .Values.elasticsearch.clusterName }}-{{ .Values.elasticsearch.nodeGroup }}
{{- else -}}
{{ .Values.elasticsearch.nameOverride }}-{{ .Values.elasticsearch.nodeGroup }}
{{- end -}}
{{- else -}}
{{ .Values.elasticsearch.fullnameOverride }}
{{- end -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "search.redis.fullname" -}}
{{- if .Values.redis.fullnameOverride -}}
{{- .Values.redis.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "redis" .Values.redis.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Get the redis password secret.
*/}}
{{- define "search.redis.secretName" -}}
{{- if .Values.redis.existingSecret -}}
{{- printf "%s" .Values.redis.existingSecret -}}
{{- else -}}
{{- printf "%s" .Values.secrets.redis.name -}}
{{- end -}}
{{- end -}}

{{/*
Get the password key to be retrieved from Redis(TM) secret.
*/}}
{{- define "search.redis.secretPasswordKey" -}}
{{- if and .Values.redis.existingSecret .Values.redis.existingSecretPasswordKey -}}
{{- printf "%s" .Values.redis.existingSecretPasswordKey -}}
{{- else -}}
{{- printf "%s" .Values.secrets.redis.passwordKey -}}
{{- end -}}
{{- end -}}

{{/*
Create a default fully qualified app name for web.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "search.tranql.fullname" -}}
{{- if .Values.tranql.fullnameOverride }}
{{- .Values.tranql.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default "tranql" .Values.tranql.nameOverride }}
{{- if contains $name .Release.Name }}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

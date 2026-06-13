{{- define "fabriq.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fabriq.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "fabriq.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fabriq.labels" -}}
helm.sh/chart: {{ include "fabriq.chart" . }}
{{ include "fabriq.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: fabriq
{{- end -}}

{{- define "fabriq.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fabriq.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "fabriq.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fabriq.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "fabriq.secretName" -}}
{{- if .Values.secret.existingSecret -}}
{{- .Values.secret.existingSecret -}}
{{- else -}}
{{- include "fabriq.fullname" . -}}
{{- end -}}
{{- end -}}

{{- define "fabriq.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Fail fast on the one mandatory input the chart cannot infer: a Postgres
DSN. When existingSecret is used, we trust it carries FABRIQ_POSTGRES_DSN.
*/}}
{{- define "fabriq.validate" -}}
{{- if and (not .Values.secret.existingSecret) (not .Values.secret.postgresDSN) -}}
{{- fail "fabriq: set secret.postgresDSN, or point secret.existingSecret at a Secret containing FABRIQ_POSTGRES_DSN (postgres is the source of truth)." -}}
{{- end -}}
{{- end -}}

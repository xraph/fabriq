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

{{- define "fabriq.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
fabriq.postgres.dsnWith composes a DSN from postgres.* parts with a caller-
supplied password token (a literal, an empty string, or a "$(VAR)"
reference resolved at runtime by Kubelet). Call with
(dict "pg" .Values.postgres "pw" <token>).
*/}}
{{- define "fabriq.postgres.dsnWith" -}}
{{- $pg := .pg -}}
{{- $auth := $pg.user -}}
{{- if .pw -}}{{- $auth = printf "%s:%s" $pg.user .pw -}}{{- end -}}
{{- $q := list -}}
{{- if $pg.sslmode -}}{{- $q = append $q (printf "sslmode=%s" $pg.sslmode) -}}{{- end -}}
{{- if $pg.params -}}{{- $q = append $q $pg.params -}}{{- end -}}
{{- $qs := "" -}}{{- if $q -}}{{- $qs = printf "?%s" (join "&" $q) -}}{{- end -}}
{{- printf "postgres://%s@%s:%v/%s%s" $auth $pg.host ($pg.port | toString) $pg.database $qs -}}
{{- end -}}

{{/*
fabriq.postgres.chartDSN is the DSN the chart materializes into its own
Secret: the literal dsn when given, else the parts composed with the
inline password. Only rendered when fabriq.postgres.needsChartSecret.
*/}}
{{- define "fabriq.postgres.chartDSN" -}}
{{- if .Values.postgres.dsn -}}
{{- .Values.postgres.dsn -}}
{{- else -}}
{{- include "fabriq.postgres.dsnWith" (dict "pg" .Values.postgres "pw" .Values.postgres.password) -}}
{{- end -}}
{{- end -}}

{{/*
fabriq.postgres.needsChartSecret is non-empty when the DSN's credentials
live in the chart's own Secret — i.e. an inline dsn or an inline password.
External-secret paths (existingSecret, passwordSecret) return "".
*/}}
{{- define "fabriq.postgres.needsChartSecret" -}}
{{- $pg := .Values.postgres -}}
{{- if and (not $pg.existingSecret) (not $pg.passwordSecret) (or $pg.dsn $pg.password) -}}true{{- end -}}
{{- end -}}

{{/*
fabriq.addrEnv emits one optional address env entry (non-sensitive
host:port). Call with (dict "var" NAME "store" .Values.<store> "key" KEY
"val" ADDR). Renders nothing when the store is unconfigured.
*/}}
{{- define "fabriq.addrEnv" -}}
{{- if .store.existingSecret }}
- name: {{ .var }}
  valueFrom:
    secretKeyRef:
      name: {{ .store.existingSecret }}
      key: {{ .key }}
{{- else if .val }}
- name: {{ .var }}
  value: {{ .val | quote }}
{{- end }}
{{- end -}}

{{/*
fabriq.connectionEnv renders the datastore connection env shared by the
worker and the migrate Job: the Postgres DSN (from a managed Secret, the
chart's Secret, a composed value, or a $(VAR) password compose) plus the
optional Redis/FalkorDB/Elasticsearch addresses.
*/}}
{{- define "fabriq.connectionEnv" -}}
{{- $pg := .Values.postgres -}}
{{- if $pg.existingSecret }}
- name: FABRIQ_POSTGRES_DSN
  valueFrom:
    secretKeyRef:
      name: {{ $pg.existingSecret }}
      key: {{ $pg.dsnKey }}
{{- else if $pg.passwordSecret }}
- name: FABRIQ_PG_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $pg.passwordSecret }}
      key: {{ $pg.passwordKey }}
- name: FABRIQ_POSTGRES_DSN
  value: {{ include "fabriq.postgres.dsnWith" (dict "pg" $pg "pw" "$(FABRIQ_PG_PASSWORD)") | quote }}
{{- else if include "fabriq.postgres.needsChartSecret" . }}
- name: FABRIQ_POSTGRES_DSN
  valueFrom:
    secretKeyRef:
      name: {{ include "fabriq.fullname" . }}
      key: FABRIQ_POSTGRES_DSN
{{- else if $pg.host }}
- name: FABRIQ_POSTGRES_DSN
  value: {{ include "fabriq.postgres.dsnWith" (dict "pg" $pg "pw" "") | quote }}
{{- end }}
{{- include "fabriq.addrEnv" (dict "var" "FABRIQ_REDIS_ADDR" "store" .Values.redis "key" .Values.redis.addrKey "val" .Values.redis.addr) }}
{{- include "fabriq.addrEnv" (dict "var" "FABRIQ_FALKORDB_ADDR" "store" .Values.falkordb "key" .Values.falkordb.addrKey "val" .Values.falkordb.addr) }}
{{- include "fabriq.addrEnv" (dict "var" "FABRIQ_ELASTICSEARCH_ADDRS" "store" .Values.elasticsearch "key" .Values.elasticsearch.addrsKey "val" .Values.elasticsearch.addrs) }}
{{- end -}}

{{/*
fabriq.validate fails the render on inconsistent datastore config: no
Postgres source of truth, a parts-composed Postgres missing a user, or an
enabled projection plane with no engine address.
*/}}
{{- define "fabriq.validate" -}}
{{- $pg := .Values.postgres -}}
{{- if not (or $pg.existingSecret $pg.dsn $pg.host) -}}
{{- fail "fabriq: configure Postgres — set postgres.existingSecret (+dsnKey), postgres.dsn, or postgres.host/.user/.database (postgres is the source of truth)." -}}
{{- end -}}
{{- if and $pg.host (not $pg.dsn) (not $pg.existingSecret) (not $pg.user) -}}
{{- fail "fabriq: postgres.host is set but postgres.user is empty (cannot compose a DSN)." -}}
{{- end -}}
{{- if and .Values.projections.graph (not (or .Values.falkordb.existingSecret .Values.falkordb.addr)) -}}
{{- fail "fabriq: projections.graph is enabled but no FalkorDB connection is set (falkordb.addr or falkordb.existingSecret)." -}}
{{- end -}}
{{- if and .Values.projections.search (not (or .Values.elasticsearch.existingSecret .Values.elasticsearch.addrs)) -}}
{{- fail "fabriq: projections.search is enabled but no Elasticsearch connection is set (elasticsearch.addrs or elasticsearch.existingSecret)." -}}
{{- end -}}
{{- end -}}

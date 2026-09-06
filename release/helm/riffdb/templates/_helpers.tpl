{{/* Chart name, overridable. */}}
{{- define "riffdb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified release name. */}}
{{- define "riffdb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains (include "riffdb.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "riffdb.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* Namespace the release targets. */}}
{{- define "riffdb.namespace" -}}
{{- default .Release.Namespace .Values.namespace.name -}}
{{- end -}}

{{/*
Server image. An empty tag resolves to the chart's appVersion so a chart
release cannot silently deploy a different RiffDB release than it declares.
*/}}
{{- define "riffdb.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{/* Proxy image, digest-pinned when a digest is supplied. */}}
{{- define "riffdb.proxyImage" -}}
{{- if .Values.proxy.image.digest -}}
{{- printf "%s:%s@%s" .Values.proxy.image.repository .Values.proxy.image.tag .Values.proxy.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.proxy.image.repository .Values.proxy.image.tag -}}
{{- end -}}
{{- end -}}

{{/* Exact in-cluster service identity used by every chart-owned client. */}}
{{- define "riffdb.serviceEndpoint" -}}
{{- printf "https://%s.%s.svc:%d" (include "riffdb.fullname" .) (include "riffdb.namespace" .) (int .Values.server.listener.port) -}}
{{- end -}}

{{/* Exact DNS SAN required on the server certificate. */}}
{{- define "riffdb.serviceDnsName" -}}
{{- printf "%s.%s.svc" (include "riffdb.fullname" .) (include "riffdb.namespace" .) -}}
{{- end -}}

{{- define "riffdb.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "riffdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "riffdb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "riffdb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cronradar-kubernetes.name" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cronradar-kubernetes.selectorLabels" -}}
app.kubernetes.io/name: cronradar-kubernetes
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cronradar-kubernetes.labels" -}}
{{ include "cronradar-kubernetes.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "cronradar-kubernetes.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}
{{- end -}}

{{- define "cronradar-kubernetes.rules" -}}
- apiGroups: ["batch"]
  resources: ["cronjobs", "jobs"]
  verbs: ["list", "watch"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["list"]
{{- end -}}

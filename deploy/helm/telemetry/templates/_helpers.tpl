{{- define "telemetry.name" -}}
{{- printf "%s-telemetry" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}
{{- define "telemetry.dbSecret" -}}
{{- if .Values.postgresql.enabled -}}
{{ include "telemetry.name" . }}-db
{{- else -}}
{{ required "externalDatabaseSecret is required when postgresql.enabled=false" .Values.externalDatabaseSecret }}
{{- end -}}
{{- end -}}

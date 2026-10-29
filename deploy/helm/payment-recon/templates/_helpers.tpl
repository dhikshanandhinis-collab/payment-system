{{- define "recon.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "recon.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "recon.selector" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "recon.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}
{{- end -}}

{{/* Shared environment: ConfigMap plus secrets passed as <NAME>_FILE paths. Args: root, admin (bool), name. */}}
{{- define "recon.env" -}}
- name: SERVICE_NAME
  value: {{ .name | quote }}
{{- range $env, $key := .root.Values.secrets.keys }}
- name: {{ $env }}_FILE
  value: /run/secrets/recon/{{ $key }}
{{- end }}
{{- if .admin }}
{{- range $env, $key := .root.Values.secrets.adminKeys }}
- name: {{ $env }}_FILE
  value: /run/secrets/recon/{{ $key }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "recon.envFrom" -}}
- configMapRef:
    name: {{ include "recon.fullname" . }}
{{- end -}}

{{/* Same settings as the ConfigMap, inline: hook Jobs run before release resources (the ConfigMap) exist. */}}
{{- define "recon.configEnv" -}}
- {name: ENVIRONMENT, value: {{ .Values.environment | quote }}}
- {name: LOG_LEVEL, value: {{ .Values.logLevel | quote }}}
- {name: BUSINESS_TIMEZONE, value: {{ .Values.businessTimezone | quote }}}
{{- range $key, $value := .Values.config }}
- {name: {{ $key }}, value: {{ $value | quote }}}
{{- end }}
{{- end -}}

{{/* Secret volume. Args: root, admin (bool). Only the keys a workload needs are projected. */}}
{{- define "recon.secretVolumes" -}}
- name: secrets
  secret:
    secretName: {{ .root.Values.secrets.existingSecret }}
    defaultMode: 0400
    items:
    {{- range $env, $key := .root.Values.secrets.keys }}
      - key: {{ $key }}
        path: {{ $key }}
    {{- end }}
    {{- if .admin }}
    {{- range $env, $key := .root.Values.secrets.adminKeys }}
      - key: {{ $key }}
        path: {{ $key }}
    {{- end }}
    {{- end }}
- name: tls
  secret:
    secretName: {{ .root.Values.tls.existingSecret }}
    defaultMode: 0444
- name: tmp
  emptyDir: {sizeLimit: 64Mi}
{{- end -}}

{{- define "recon.secretMounts" -}}
- name: secrets
  mountPath: /run/secrets/recon
  readOnly: true
- name: tls
  mountPath: /etc/recon/tls
  readOnly: true
- name: tmp
  mountPath: /tmp
{{- end -}}

{{- define "recon.podSpecCommon" -}}
securityContext:
  {{- toYaml .Values.podSecurityContext | nindent 2 }}
automountServiceAccountToken: false
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- define "schedulerengine.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "schedulerengine.fullname" -}}
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

{{- define "schedulerengine.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "schedulerengine.version" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "schedulerengine.ingress" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Values.ingress.path}}
{{- else -}}
{{- printf "/%s%s" .Release.Name .Values.ingress.path | trunc 63 | trimSuffix "/" -}}
{{- end -}}
{{- end -}}

{{- define "portal-admin-usecases.keycloak.service.fullname" -}}
{{- if .Values.keycloak.service.fullnameOverride -}}
{{- .Values.keycloak.service.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-ckey" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "portal-admin-usecases.portal.adminSecret" -}}
{{- if .Values.keycloak.service.portalAdminSecretNameOverride -}}
{{- .Values.keycloak.service.portalAdminSecretNameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- if eq .Values.application.environment "test" }}
{{- .Values.app.testPortalKeyCloakSecrets}}
{{- else -}}
{{- printf "portal-keycloak-config-secrets" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

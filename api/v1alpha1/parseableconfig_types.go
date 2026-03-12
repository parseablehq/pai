/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NamespaceSelector defines which namespaces to include or exclude
type NamespaceSelector struct {
	// Mode specifies whether to include or exclude the listed namespaces
	// +kubebuilder:validation:Enum=include;exclude
	Mode string `json:"mode,omitempty"`

	// Namespaces is the list of namespace names
	Namespaces []string `json:"namespaces,omitempty"`
}

// SecretReference references a Kubernetes secret
type SecretReference struct {
	// Name is the name of the secret
	Name string `json:"name"`

	// Namespace is the namespace of the secret
	Namespace string `json:"namespace"`
}

// StreamConfig defines the stream names for different telemetry types
type StreamConfig struct {
	// Logs is the stream name for log data
	Logs string `json:"logs,omitempty"`

	// Traces is the stream name for trace data
	Traces string `json:"traces,omitempty"`

	// Metrics is the stream name for metric data
	Metrics string `json:"metrics,omitempty"`
}

// TargetConfig defines the Parseable target endpoint configuration
type TargetConfig struct {
	// Endpoint is the Parseable API endpoint URL for ingestion
	Endpoint string `json:"endpoint"`

	// CredentialsSecret references the secret containing authentication credentials
	CredentialsSecret SecretReference `json:"credentialsSecret"`

	// Streams defines the stream names for different telemetry types
	Streams StreamConfig `json:"streams,omitempty"`
}

// InstrumentationConfig defines auto-instrumentation settings
type InstrumentationConfig struct {
	// Mode specifies the instrumentation mode
	// +kubebuilder:validation:Enum=auto;manual;hybrid
	Mode string `json:"mode,omitempty"`

	// Languages specifies which programming languages to instrument
	// +kubebuilder:validation:Items:Enum=java;python;nodejs;go
	Languages []string `json:"languages,omitempty"`

	// LanguageDetection specifies how to detect application languages
	// +kubebuilder:validation:Enum=auto;manual
	LanguageDetection string `json:"languageDetection,omitempty"`
}

// LogEnrichment defines log enrichment options
type LogEnrichment struct {
	// KubernetesMetadata enables enriching logs with Kubernetes metadata
	KubernetesMetadata bool `json:"kubernetesMetadata,omitempty"`
}

// LogsConfig defines logging configuration
type LogsConfig struct {
	// Enabled specifies whether log collection is enabled
	Enabled bool `json:"enabled,omitempty"`

	// Collector specifies the log collector to use
	Collector string `json:"collector,omitempty"`

	// Enrichment defines log enrichment options
	Enrichment LogEnrichment `json:"enrichment,omitempty"`
}

// NodeMetricsConfig defines node-level metrics collection
type NodeMetricsConfig struct {
	// Enabled specifies whether node metrics collection is enabled
	Enabled bool `json:"enabled,omitempty"`

	// ScrapeInterval is the interval at which to scrape metrics
	ScrapeInterval string `json:"scrapeInterval,omitempty"`

	// Collectors specifies which metric collectors to enable
	// +kubebuilder:validation:Items:Enum=cpu;memory;disk;network
	Collectors []string `json:"collectors,omitempty"`
}

// KubernetesEventsConfig defines Kubernetes event collection
type KubernetesEventsConfig struct {
	// Enabled specifies whether Kubernetes event collection is enabled
	Enabled bool `json:"enabled,omitempty"`

	// EventTypes specifies which event types to collect
	// +kubebuilder:validation:Items:Enum=Normal;Warning
	EventTypes []string `json:"eventTypes,omitempty"`
}

// DaemonSetResources defines resources for DaemonSet workloads
type DaemonSetResources struct {
	// Limits describes the maximum amount of compute resources allowed
	Limits corev1.ResourceList `json:"limits,omitempty"`
}

// ResourcesConfig defines resource configurations for operator-managed workloads
type ResourcesConfig struct {
	// DaemonSet defines resources for DaemonSet workloads
	DaemonSet DaemonSetResources `json:"daemonSet,omitempty"`
}

// ParseableConfigSpec defines the desired state of ParseableConfig
type ParseableConfigSpec struct {
	// NamespaceSelector defines which namespaces to include or exclude from instrumentation
	NamespaceSelector NamespaceSelector `json:"namespaceSelector,omitempty"`

	// Target defines the Parseable endpoint configuration
	Target TargetConfig `json:"target"`

	// Instrumentation defines auto-instrumentation settings
	Instrumentation InstrumentationConfig `json:"instrumentation,omitempty"`

	// Logs defines logging configuration
	Logs LogsConfig `json:"logs,omitempty"`

	// NodeMetrics defines node-level metrics collection settings
	NodeMetrics NodeMetricsConfig `json:"nodeMetrics,omitempty"`

	// KubernetesEvents defines Kubernetes event collection settings
	KubernetesEvents KubernetesEventsConfig `json:"kubernetesEvents,omitempty"`

	// Resources defines resource configurations for operator-managed workloads
	Resources ResourcesConfig `json:"resources,omitempty"`
}

// ParseableConfigStatus defines the observed state of ParseableConfig
type ParseableConfigStatus struct {
	// Conditions represent the latest available observations of the resource's state
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// ParseableConfig is the Schema for the parseableconfigs API
type ParseableConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ParseableConfigSpec   `json:"spec,omitempty"`
	Status ParseableConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ParseableConfigList contains a list of ParseableConfig
type ParseableConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ParseableConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ParseableConfig{}, &ParseableConfigList{})
}

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

// TargetConfig defines the global Parseable target endpoint configuration
type TargetConfig struct {
	// Endpoint is the Parseable API endpoint URL for ingestion
	Endpoint string `json:"endpoint"`

	// CredentialsSecret references the secret containing authentication credentials
	CredentialsSecret SecretReference `json:"credentialsSecret"`

	// GlobalTenantID is an optional tenant identifier. When set, the X-P-Tenant header is added to all collector exporters.
	GlobalTenantID string `json:"globalTenantId,omitempty"`

	// Headers are additional HTTP headers applied to all signal exporters. Signal-level headers override these.
	Headers map[string]string `json:"headers,omitempty"`

	// Encoding controls the OTLP HTTP wire format. "json" (default) is universally supported by Parseable;
	// "proto" is more efficient but not every Parseable cluster supports it.
	// +kubebuilder:validation:Enum=json;proto
	Encoding string `json:"encoding,omitempty"`
}

// InstrumentationConfig defines auto-instrumentation settings
type InstrumentationConfig struct {
	// Languages specifies which programming languages to instrument
	// +kubebuilder:validation:Items:Enum=java;python;nodejs;dotnet;go
	Languages []string `json:"languages,omitempty"`

	// DetectionTimeout is how long (e.g. "60s", "2m") to wait during auto-detection.
	// Defaults to "1m" if not set.
	DetectionTimeout string `json:"detectionTimeout,omitempty"`
}

// WorkloadSelector defines which workloads to include or exclude by labels
type WorkloadSelector struct {
	// Mode specifies whether to include or exclude workloads matching the selector
	// +kubebuilder:validation:Enum=include;exclude
	Mode string `json:"mode,omitempty"`

	// LabelSelector is a standard Kubernetes label selector supporting matchLabels and matchExpressions
	metav1.LabelSelector `json:",inline"`
}

// TracesConfig defines tracing configuration
type TracesConfig struct {
	// TargetDataset is the Parseable dataset name for trace data
	TargetDataset string `json:"targetDataset"`

	// Headers are additional HTTP headers for the traces exporter. Overrides global headers with the same key.
	Headers map[string]string `json:"headers,omitempty"`

	// NamespaceSelector defines which namespaces to target for tracing
	NamespaceSelector NamespaceSelector `json:"namespaceSelector,omitempty"`

	// WorkloadSelector defines which workloads to include or exclude by labels
	WorkloadSelector *WorkloadSelector `json:"workloadSelector,omitempty"`

	// Instrumentation defines auto-instrumentation settings for traces
	Instrumentation InstrumentationConfig `json:"instrumentation,omitempty"`
}

// LogsConfig defines logging configuration: a built-in toggle for cluster pod
// logs plus an array of host-path tail pipelines for arbitrary log directories.
type LogsConfig struct {
	// PodLogs enables collection of all Kubernetes pod logs from /var/log/pods on each node.
	PodLogs *PodLogsConfig `json:"podLogs,omitempty"`

	// Files is a list of host-path tail pipelines (e.g. audit logs, server logs).
	Files []FileLogConfig `json:"files,omitempty"`
}

// PodLogsConfig configures collection of Kubernetes pod logs via the filelog
// receiver with CRI container parsing and optional namespace filtering.
type PodLogsConfig struct {
	// Enabled controls whether Kubernetes pod logs are collected
	Enabled bool `json:"enabled"`

	// TargetDataset is the Parseable dataset name for pod log data (required when Enabled is true)
	TargetDataset string `json:"targetDataset,omitempty"`

	// Headers are additional HTTP headers for the pod logs exporter. Overrides global headers with the same key.
	Headers map[string]string `json:"headers,omitempty"`

	// NamespaceSelector defines which namespaces to collect pod logs from
	NamespaceSelector NamespaceSelector `json:"namespaceSelector,omitempty"`
}

// FileLogConfig defines a host-path tail pipeline. Every *.log file under
// HostPath (recursive) is tailed without CRI/container parsing.
type FileLogConfig struct {
	// Name uniquely identifies this file pipeline; used to name the receiver, exporter, volume, and pipeline.
	Name string `json:"name"`

	// HostPath is the directory on the node to mount and tail recursively
	HostPath string `json:"hostPath"`

	// TargetDataset is the Parseable dataset name for this pipeline's log data
	TargetDataset string `json:"targetDataset"`

	// Headers are additional HTTP headers for this exporter. Overrides global headers with the same key.
	Headers map[string]string `json:"headers,omitempty"`
}

// ClusterMetricsConfig enables built-in cluster-wide metrics from up to three
// receivers (k8s_cluster, kubelet /metrics, kube-state-metrics). All enabled
// receivers ship to the same TargetDataset.
type ClusterMetricsConfig struct {
	// TargetDataset is the Parseable dataset name shared by every enabled built-in receiver
	TargetDataset string `json:"targetDataset"`

	// Headers are additional HTTP headers for the cluster metrics exporter. Overrides global headers with the same key.
	Headers map[string]string `json:"headers,omitempty"`

	// NamespaceSelector filters pod-scope metrics by namespace. Node-scope metrics are not filtered.
	NamespaceSelector NamespaceSelector `json:"namespaceSelector,omitempty"`

	// K8sCluster collects cluster object state (pod/deployment status, node conditions) via the k8s_cluster receiver.
	K8sCluster *K8sClusterConfig `json:"k8sCluster,omitempty"`

	// Kubelet scrapes each node's kubelet /metrics endpoint (Prometheus format, TLS + service-account bearer).
	Kubelet *KubeletConfig `json:"kubelet,omitempty"`

	// KubeState scrapes the kube-state-metrics service via Kubernetes service discovery.
	KubeState *KubeStateConfig `json:"kubeState,omitempty"`
}

// K8sClusterConfig configures the k8s_cluster receiver.
type K8sClusterConfig struct {
	// Enabled controls whether the k8s_cluster receiver runs
	Enabled bool `json:"enabled"`

	// NodeConditions is the list of node conditions to report (e.g. Ready, DiskPressure, MemoryPressure).
	// Defaults to ["Ready"] when empty.
	NodeConditions []string `json:"nodeConditions,omitempty"`

	// AllocatableResources is the list of allocatable resources to report (e.g. cpu, memory, storage).
	// Defaults to no allocatable metrics when empty.
	AllocatableResources []string `json:"allocatableResources,omitempty"`
}

// KubeletConfig configures the prometheus scrape of each node's kubelet /metrics endpoint.
type KubeletConfig struct {
	// Enabled controls whether kubelet /metrics scraping runs
	Enabled bool `json:"enabled"`
}

// KubeStateConfig configures the prometheus scrape of the kube-state-metrics service.
type KubeStateConfig struct {
	// Enabled controls whether kube-state-metrics scraping runs
	Enabled bool `json:"enabled"`

	// Namespaces restricts where the kube-state-metrics service is discovered.
	// Defaults to ["kube-system", "kube-state-metrics", "default"] when empty.
	Namespaces []string `json:"namespaces,omitempty"`
}

// ScrapeConfig defines a single Prometheus-style scrape pipeline. Pods are
// discovered via Kubernetes service discovery and scraped at the given path+port.
// Two pod-selection modes are supported:
//   - PodSelector (label match) — recommended; matches pods by label key/value
//   - port-only — keeps pods whose container exposes the named port (legacy)
type ScrapeConfig struct {
	// Name uniquely identifies this scrape pipeline
	Name string `json:"name"`

	// URI is the HTTP path to scrape on each discovered pod (e.g. "/metrics")
	URI string `json:"uri"`

	// Port is the container port to scrape
	Port int32 `json:"port"`

	// TargetDataset is the Parseable dataset name for this scrape pipeline's metric data
	TargetDataset string `json:"targetDataset"`

	// Headers are additional HTTP headers for this exporter. Overrides global headers with the same key.
	Headers map[string]string `json:"headers,omitempty"`

	// NamespaceSelector limits service discovery to the matching namespaces
	NamespaceSelector NamespaceSelector `json:"namespaceSelector,omitempty"`

	// PodSelector selects pods by label key/value pairs. When set, the operator emits
	// a Prometheus keep-relabel per label and skips the port-number filter.
	PodSelector map[string]string `json:"podSelector,omitempty"`
}

// MetricsConfig defines metrics configuration. ClusterMetrics enables built-in
// kubelet/cluster metrics; ScrapeConfigs adds Prometheus-style scrape pipelines.
type MetricsConfig struct {
	// ClusterMetrics toggles built-in node/pod/cluster metrics via kubeletstats + k8s_cluster receivers
	ClusterMetrics *ClusterMetricsConfig `json:"clusterMetrics,omitempty"`

	// ScrapeConfigs is a list of Prometheus-style scrape pipelines
	ScrapeConfigs []ScrapeConfig `json:"scrapeConfigs,omitempty"`
}

// EventsConfig defines Kubernetes events collection configuration
type EventsConfig struct {
	// Enabled controls whether Kubernetes events are collected
	Enabled bool `json:"enabled"`

	// TargetDataset is the Parseable dataset name for event data
	TargetDataset string `json:"targetDataset,omitempty"`

	// Headers are additional HTTP headers for the events exporter. Overrides global headers with the same key.
	Headers map[string]string `json:"headers,omitempty"`

	// NamespaceSelector defines which namespaces to collect events from
	NamespaceSelector NamespaceSelector `json:"namespaceSelector,omitempty"`
}

// ParseableConfigSpec defines the desired state of ParseableConfig
type ParseableConfigSpec struct {
	// Paused stops all data collection when set to true.
	// Collectors, instrumentation, and agent are deleted. Set to false to resume.
	Paused bool `json:"paused,omitempty"`

	// Target defines the global Parseable endpoint and credentials
	Target TargetConfig `json:"target"`

	// Traces defines tracing configuration
	Traces *TracesConfig `json:"traces,omitempty"`

	// Logs defines logging configuration (built-in pod logs toggle + host-path tail pipelines)
	Logs *LogsConfig `json:"logs,omitempty"`

	// Metrics defines metrics configuration (cluster metrics toggle + scrape configs)
	Metrics *MetricsConfig `json:"metrics,omitempty"`

	// Events defines Kubernetes events collection configuration
	Events *EventsConfig `json:"events,omitempty"`
}

// WorkloadInstrumentationStatus tracks the detection result for a single workload
type WorkloadInstrumentationStatus struct {
	// Name of the workload
	Name string `json:"name"`

	// Namespace of the workload
	Namespace string `json:"namespace"`

	// Kind is Deployment or StatefulSet
	Kind string `json:"kind"`

	// DetectedLanguage is the language that was detected (empty if none matched)
	DetectedLanguage string `json:"detectedLanguage,omitempty"`

	// Instrumented indicates whether the workload was successfully instrumented
	Instrumented bool `json:"instrumented"`

	// LastDetectionTime is when detection was last performed
	LastDetectionTime *metav1.Time `json:"lastDetectionTime,omitempty"`

	// ContainerImage is the image that was running when detection was performed
	ContainerImage string `json:"containerImage,omitempty"`

	// ObservedGeneration is the CR generation when this workload was last processed
	ObservedGeneration int64 `json:"observedGeneration"`
}

// ParseableConfigStatus defines the observed state of ParseableConfig
type ParseableConfigStatus struct {
	// Conditions represent the latest available observations of the resource's state
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Workloads tracks the instrumentation status of each processed workload
	Workloads []WorkloadInstrumentationStatus `json:"workloads,omitempty"`
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

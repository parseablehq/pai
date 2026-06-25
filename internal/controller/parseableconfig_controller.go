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

package controller

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	observabilityv1alpha1 "github.com/parseable/pai/api/v1alpha1"
)

const (
	instrumentationName = "pai-instrumentation"

	// kept for cleanup of legacy sidecar resources
	sidecarCollectorName = "pai-sidecar"
	sidecarAnnotation    = "sidecar.opentelemetry.io/inject"

	logCollectorName           = "pai-log-collector"
	metricsEventsCollectorName = "pai-metrics-events-collector"
	collectorClusterRoleName   = "pai-collector"
	paiAgentDaemonSetName      = "pai-agent"

	instrumentationAnnotPrefix = "instrumentation.opentelemetry.io/inject-"

	finalizerName = "observability.parseable.com/finalizer"
)

// language binary checks for exec-based detection
var languageBinaryChecks = map[string][][]string{
	"java":   {{"java", "-version"}},
	"nodejs": {{"node", "--version"}},
	"python": {{"python3", "--version"}, {"python", "--version"}},
	"dotnet": {{"dotnet", "--version"}},
}

// imageHeuristics maps container image name patterns to languages.
// Checked in order; first match wins. Patterns are matched against the image
// name (without tag) in lowercase.
var imageHeuristics = []struct {
	patterns []string
	language string
}{
	{patterns: []string{"openjdk", "eclipse-temurin", "amazoncorretto", "azul/zulu", "liberica", "graalvm"}, language: "java"},
	{patterns: []string{"python", "django", "flask", "fastapi"}, language: "python"},
	{patterns: []string{"node", "nodejs"}, language: "nodejs"},
	{patterns: []string{"dotnet", "aspnet", "mcr.microsoft.com/dotnet"}, language: "dotnet"},
}

// auto-instrumentation images per language
// Note: Go auto-instrumentation requires enabling a feature flag on the OTel operator
var languageImages = map[string]string{
	"java":   "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-java:latest",
	"python": "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-python:latest",
	"nodejs": "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-nodejs:latest",
	"dotnet": "ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-dotnet:latest",
}

// ParseableConfigReconciler reconciles a ParseableConfig object
type ParseableConfigReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	RestConfig *rest.Config
	Clientset  kubernetes.Interface
}

// +kubebuilder:rbac:groups=observability.parseable.com,resources=parseableconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=observability.parseable.com,resources=parseableconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=observability.parseable.com,resources=parseableconfigs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/proxy,verbs=get
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=opentelemetry.io,resources=opentelemetrycollectors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=opentelemetry.io,resources=instrumentations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *ParseableConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the ParseableConfig instance
	config := &observabilityv1alpha1.ParseableConfig{}
	if err := r.Get(ctx, req.NamespacedName, config); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion with finalizer
	if !config.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(config, finalizerName) {
			logger.Info("Cleaning up resources before deletion", "name", config.Name)
			if err := r.cleanup(ctx, config); err != nil {
				logger.Error(err, "Failed to cleanup resources")
				return ctrl.Result{}, err
			}

			controllerutil.RemoveFinalizer(config, finalizerName)
			if err := r.Update(ctx, config); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("Cleanup complete, finalizer removed", "name", config.Name)
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(config, finalizerName) {
		controllerutil.AddFinalizer(config, finalizerName)
		if err := r.Update(ctx, config); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("Finalizer added", "name", config.Name)
	}

	// Handle paused state — delete all collectors and stop reconciliation
	if config.Spec.Paused {
		logger.Info("ParseableConfig is paused, removing all collectors and instrumentation")
		if err := r.cleanup(ctx, config); err != nil {
			logger.Error(err, "Failed to cleanup resources for pause")
		}
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling Pai config", "name", config.Name)

	// Step 0: Ensure namespace exists
	ns := &corev1.Namespace{}
	if err := r.Get(ctx, client.ObjectKey{Name: config.Namespace}, ns); err != nil {
		if errors.IsNotFound(err) {
			ns = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: config.Namespace}}
			if err := r.Create(ctx, ns); err != nil && !errors.IsAlreadyExists(err) {
				return ctrl.Result{}, fmt.Errorf("failed to create namespace %s: %w", config.Namespace, err)
			}
			logger.Info("Namespace created", "namespace", config.Namespace)
		} else {
			return ctrl.Result{}, err
		}
	}

	// Step 1: Ensure Instrumentation CR exists (sends traces directly to Parseable)
	if err := r.ensureInstrumentation(ctx, config); err != nil {
		logger.Error(err, "Failed to ensure Instrumentation CR")
		return ctrl.Result{}, err
	}

	// Step 3: Ensure PAI agent DaemonSet for host-level process detection (distroless support)
	if err := r.ensurePaiAgent(ctx, config); err != nil {
		logger.Error(err, "Failed to ensure PAI agent DaemonSet")
		return ctrl.Result{}, err
	}

	// Step 4: Detect languages via exec and annotate workloads (single rollout per workload)
	if err := r.ensureAnnotations(ctx, config); err != nil {
		logger.Error(err, "Failed to ensure annotations")
		return ctrl.Result{}, err
	}

	// Step 5: Ensure collector RBAC (ClusterRole + bindings for collector ServiceAccounts)
	if err := r.ensureCollectorRBAC(ctx, config.Namespace); err != nil {
		logger.Error(err, "Failed to ensure collector RBAC")
		return ctrl.Result{}, err
	}

	// Step 6: Ensure log collector DaemonSet if logs are enabled
	if err := r.ensureLogCollector(ctx, config); err != nil {
		logger.Error(err, "Failed to ensure log collector")
		return ctrl.Result{}, err
	}

	// Step 7: Ensure metrics+events collector Deployment
	if err := r.ensureMetricsEventsCollector(ctx, config); err != nil {
		logger.Error(err, "Failed to ensure metrics/events collector")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// ensureCollectorRBAC creates a ClusterRole and ClusterRoleBindings for collector ServiceAccounts.
// The k8s_cluster, k8sobjects, and kubeletstats receivers need permissions to list/watch cluster resources.
func (r *ParseableConfigReconciler) ensureCollectorRBAC(ctx context.Context, namespace string) error {
	logger := log.FromContext(ctx)

	// Create or update ClusterRole (namespaced name to avoid conflicts)
	clusterRoleName := namespace + "-" + collectorClusterRoleName
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterRoleName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"events", "namespaces", "namespaces/status",
					"nodes", "nodes/spec", "nodes/stats", "nodes/proxy", "nodes/metrics",
					"pods", "pods/status", "replicationcontrollers", "replicationcontrollers/status",
					"resourcequotas", "services"},
				Verbs: []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"daemonsets", "deployments", "replicasets", "statefulsets"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{"batch"},
				Resources: []string{"jobs", "cronjobs"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{"autoscaling"},
				Resources: []string{"horizontalpodautoscalers"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}

	existing := &rbacv1.ClusterRole{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(role), existing); err != nil {
		if errors.IsNotFound(err) {
			if err := r.Create(ctx, role); err != nil {
				return fmt.Errorf("failed to create ClusterRole: %w", err)
			}
			logger.Info("ClusterRole created", "name", clusterRoleName)
		} else {
			return err
		}
	} else {
		existing.Rules = role.Rules
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update ClusterRole: %w", err)
		}
	}

	// Bind each collector ServiceAccount (OTel operator auto-creates them as <collector-name>-collector)
	serviceAccounts := []string{
		logCollectorName + "-collector",
		metricsEventsCollectorName + "-collector",
	}

	for _, sa := range serviceAccounts {
		binding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace + "-" + sa,
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     clusterRoleName,
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      sa,
					Namespace: namespace,
				},
			},
		}

		existingBinding := &rbacv1.ClusterRoleBinding{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(binding), existingBinding); err != nil {
			if errors.IsNotFound(err) {
				if err := r.Create(ctx, binding); err != nil {
					return fmt.Errorf("failed to create ClusterRoleBinding for %s: %w", sa, err)
				}
				logger.Info("ClusterRoleBinding created", "serviceAccount", sa)
			} else {
				return err
			}
		} else {
			existingBinding.Subjects = binding.Subjects
			if err := r.Update(ctx, existingBinding); err != nil {
				return fmt.Errorf("failed to update ClusterRoleBinding for %s: %w", sa, err)
			}
		}
	}

	return nil
}

// buildInstrumentationSpec builds the Instrumentation spec that sends traces directly to Parseable
func (r *ParseableConfigReconciler) buildInstrumentationSpec(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) (map[string]interface{}, error) {
	// Read credentials secret
	secret := &corev1.Secret{}
	secretRef := config.Spec.Target.CredentialsSecret
	if err := r.Get(ctx, client.ObjectKey{Name: secretRef.Name, Namespace: secretRef.Namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to read credentials secret %s/%s: %w", secretRef.Namespace, secretRef.Name, err)
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	basicAuth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))

	tracesStream := config.Spec.Traces.TargetDataset
	endpoint := strings.TrimRight(config.Spec.Target.Endpoint, "/")

	spec := map[string]interface{}{
		"exporter": map[string]interface{}{
			"endpoint": endpoint,
		},
		"propagators": []interface{}{
			"tracecontext",
			"baggage",
		},
		"env": []interface{}{
			map[string]interface{}{
				"name":  "OTEL_EXPORTER_OTLP_PROTOCOL",
				"value": resolveOtlpProtocol(config.Spec.Target.Encoding),
			},
			map[string]interface{}{
				"name":  "OTEL_EXPORTER_OTLP_HEADERS",
				"value": r.buildOtlpHeaders(basicAuth, "otel-traces", tracesStream, config.Spec.Target.GlobalTenantID, config.Spec.Target.Headers, config.Spec.Traces.Headers),
			},
		},
	}

	for _, lang := range config.Spec.Traces.Instrumentation.Languages {
		image, ok := languageImages[lang]
		if !ok {
			continue
		}
		spec[lang] = map[string]interface{}{
			"image": image,
		}
	}

	return spec, nil
}

// ensureInstrumentation creates or updates the Instrumentation CR with language sections based on the ParseableConfig
func (r *ParseableConfigReconciler) ensureInstrumentation(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	if config.Spec.Traces == nil || len(config.Spec.Traces.Instrumentation.Languages) == 0 {
		logger.Info("No languages configured, skipping Instrumentation CR")
		return nil
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1alpha1",
		Kind:    "Instrumentation",
	})

	err := r.Get(ctx, client.ObjectKey{Name: instrumentationName, Namespace: config.Namespace}, existing)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check Instrumentation CR: %w", err)
	}

	spec, specErr := r.buildInstrumentationSpec(ctx, config)
	if specErr != nil {
		return specErr
	}

	if err == nil {
		// Update existing CR with current languages
		existing.Object["spec"] = spec
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update Instrumentation CR: %w", err)
		}
		logger.Info("Instrumentation CR updated", "languages", config.Spec.Traces.Instrumentation.Languages)
		return nil
	}

	// Create new
	logger.Info("Creating Instrumentation CR", "languages", config.Spec.Traces.Instrumentation.Languages)
	instrumentation := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "opentelemetry.io/v1alpha1",
			"kind":       "Instrumentation",
			"metadata": map[string]interface{}{
				"name":      instrumentationName,
				"namespace": config.Namespace,
			},
			"spec": spec,
		},
	}

	if err := r.Create(ctx, instrumentation); err != nil {
		return fmt.Errorf("failed to create Instrumentation CR: %w", err)
	}

	logger.Info("Instrumentation CR created successfully", "name", instrumentationName, "namespace", config.Namespace)
	return nil
}

// ensureLogCollector creates or updates a DaemonSet-mode OpenTelemetryCollector CR for log collection.
// The DaemonSet hosts filelog pipelines (one per Logs[] entry) and, when ClusterMetrics is enabled,
// a kubeletstats pipeline that scrapes each node's local kubelet for pod+node resource metrics.
// If neither is configured, it deletes any existing log collector.
func (r *ParseableConfigReconciler) ensureLogCollector(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	gvk := schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(gvk)
	err := r.Get(ctx, client.ObjectKey{Name: logCollectorName, Namespace: config.Namespace}, existing)

	podLogsEnabled := config.Spec.Logs != nil &&
		config.Spec.Logs.PodLogs != nil &&
		config.Spec.Logs.PodLogs.Enabled &&
		config.Spec.Logs.PodLogs.TargetDataset != ""

	hasFiles := false
	if config.Spec.Logs != nil {
		for _, f := range config.Spec.Logs.Files {
			if f.Name != "" && f.HostPath != "" && f.TargetDataset != "" {
				hasFiles = true
				break
			}
		}
	}

	kubeletstatsEnabled := config.Spec.Metrics != nil &&
		config.Spec.Metrics.ClusterMetrics != nil &&
		config.Spec.Metrics.ClusterMetrics.Kubelet != nil &&
		config.Spec.Metrics.ClusterMetrics.Kubelet.Enabled &&
		config.Spec.Metrics.ClusterMetrics.TargetDataset != ""

	if !podLogsEnabled && !hasFiles && !kubeletstatsEnabled {
		if err == nil {
			logger.Info("Logs and kubeletstats not configured, deleting log collector")
			if delErr := r.Delete(ctx, existing); delErr != nil && !errors.IsNotFound(delErr) {
				return fmt.Errorf("failed to delete log collector: %w", delErr)
			}
		}
		return nil
	}

	collectorConfig, cfgErr := r.buildLogCollectorConfig(ctx, config)
	if cfgErr != nil {
		return cfgErr
	}

	// Build hostPath volumes/mounts: /var/log/pods for podLogs, plus each Files entry.
	volumes := []interface{}{}
	volumeMounts := []interface{}{}
	seen := map[string]bool{}
	addVolume := func(name, hostPath string) {
		if hostPath == "" || seen[hostPath] {
			return
		}
		seen[hostPath] = true
		volumes = append(volumes, map[string]interface{}{
			"name":     name,
			"hostPath": map[string]interface{}{"path": hostPath},
		})
		volumeMounts = append(volumeMounts, map[string]interface{}{
			"name":      name,
			"mountPath": hostPath,
			"readOnly":  true,
		})
	}
	if podLogsEnabled {
		addVolume("pod-logs", "/var/log/pods")
	}
	if config.Spec.Logs != nil {
		for _, f := range config.Spec.Logs.Files {
			volName := sanitizeName(f.Name)
			if volName == "" {
				volName = sanitizeName(f.HostPath)
			}
			addVolume(volName, f.HostPath)
		}
	}

	spec := map[string]interface{}{
		"mode":   "daemonset",
		"config": collectorConfig,
		// Tolerate every taint so the log DaemonSet can run on every node — required to
		// collect cluster-wide pod logs and host-path logs that live on tainted nodepools.
		"tolerations": []interface{}{
			map[string]interface{}{"operator": "Exists"},
		},
	}
	if len(volumes) > 0 {
		spec["volumes"] = volumes
		spec["volumeMounts"] = volumeMounts
	}
	// kubeletstats receiver needs the local node name to target the right kubelet API
	// (https://${env:K8S_NODE_NAME}:10250). Injected via downward API.
	if kubeletstatsEnabled {
		spec["env"] = []interface{}{
			map[string]interface{}{
				"name": "K8S_NODE_NAME",
				"valueFrom": map[string]interface{}{
					"fieldRef": map[string]interface{}{
						"fieldPath": "spec.nodeName",
					},
				},
			},
		}
	}

	if err == nil {
		// Update existing
		existing.Object["spec"] = spec
		if updErr := r.Update(ctx, existing); updErr != nil {
			return fmt.Errorf("failed to update log collector: %w", updErr)
		}
		logger.Info("Log collector updated")
		return nil
	}

	if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check log collector: %w", err)
	}

	// Create new
	logger.Info("Creating log collector DaemonSet")
	collector := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "opentelemetry.io/v1beta1",
			"kind":       "OpenTelemetryCollector",
			"metadata": map[string]interface{}{
				"name":      logCollectorName,
				"namespace": config.Namespace,
			},
			"spec": spec,
		},
	}

	if crErr := r.Create(ctx, collector); crErr != nil {
		return fmt.Errorf("failed to create log collector: %w", crErr)
	}

	logger.Info("Log collector created successfully")
	return nil
}

// buildLogCollectorConfig builds the OTel collector pipeline config for the log DaemonSet.
// Each Logs[] entry becomes its own filelog receiver + exporter pair. If ClusterMetrics is enabled,
// a single kubeletstats receiver feeds pod+node resource metrics into the same target dataset.
func (r *ParseableConfigReconciler) buildLogCollectorConfig(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) (map[string]interface{}, error) {
	secret := &corev1.Secret{}
	secretRef := config.Spec.Target.CredentialsSecret
	if err := r.Get(ctx, client.ObjectKey{Name: secretRef.Name, Namespace: secretRef.Namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to read credentials secret %s/%s: %w", secretRef.Namespace, secretRef.Name, err)
	}
	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	basicAuth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
	endpoint := strings.TrimRight(config.Spec.Target.Endpoint, "/")
	encoding := resolveOtlpEncoding(config.Spec.Target.Encoding)
	tenantID := config.Spec.Target.GlobalTenantID

	receivers := map[string]interface{}{}
	processors := map[string]interface{}{
		"batch": map[string]interface{}{},
		"k8sattributes": map[string]interface{}{
			"extract": map[string]interface{}{
				"metadata": []interface{}{
					"k8s.namespace.name",
					"k8s.pod.name",
					"k8s.container.name",
					"k8s.node.name",
				},
			},
		},
	}
	exporters := map[string]interface{}{}
	pipelines := map[string]interface{}{}

	// Pod logs — built-in pipeline tailing /var/log/pods with the CRI container parser.
	if config.Spec.Logs != nil &&
		config.Spec.Logs.PodLogs != nil &&
		config.Spec.Logs.PodLogs.Enabled &&
		config.Spec.Logs.PodLogs.TargetDataset != "" {

		pl := config.Spec.Logs.PodLogs

		var includePatterns, excludePatterns []interface{}
		switch pl.NamespaceSelector.Mode {
		case "include":
			for _, ns := range pl.NamespaceSelector.Namespaces {
				includePatterns = append(includePatterns, fmt.Sprintf("/var/log/pods/%s_*/*/*.log", ns))
			}
		case "exclude":
			includePatterns = []interface{}{"/var/log/pods/*/*/*.log"}
			for _, ns := range pl.NamespaceSelector.Namespaces {
				excludePatterns = append(excludePatterns, fmt.Sprintf("/var/log/pods/%s_*/*/*.log", ns))
			}
		default:
			includePatterns = []interface{}{"/var/log/pods/*/*/*.log"}
		}

		filelogReceiver := map[string]interface{}{
			"include":           includePatterns,
			"include_file_path": true,
			"operators": []interface{}{
				map[string]interface{}{
					"type": "container",
					"id":   "container-parser",
				},
			},
		}
		if len(excludePatterns) > 0 {
			filelogReceiver["exclude"] = excludePatterns
		}

		receivers["filelog/pod-logs"] = filelogReceiver
		exporters["otlphttp/logs_pod-logs"] = map[string]interface{}{
			"endpoint": endpoint,
			"encoding": encoding,
			"headers":  r.buildExporterHeaders(basicAuth, "otel-logs", pl.TargetDataset, tenantID, config.Spec.Target.Headers, pl.Headers),
		}
		pipelines["logs/pod-logs"] = map[string]interface{}{
			"receivers":  []interface{}{"filelog/pod-logs"},
			"processors": []interface{}{"k8sattributes", "batch"},
			"exporters":  []interface{}{"otlphttp/logs_pod-logs"},
		}
	}

	// File pipelines — one per Files[] entry. Tail every *.log under HostPath recursively, no parser.
	if config.Spec.Logs != nil {
		for _, f := range config.Spec.Logs.Files {
			id := sanitizeName(f.Name)
			if id == "" || f.HostPath == "" || f.TargetDataset == "" {
				continue
			}
			base := strings.TrimRight(f.HostPath, "/")

			receivers["filelog/"+id] = map[string]interface{}{
				"include": []interface{}{
					fmt.Sprintf("%s/*", base),
					fmt.Sprintf("%s/**/*", base),
				},
				"include_file_path": true,
			}
			exporters["otlphttp/logs_"+id] = map[string]interface{}{
				"endpoint": endpoint,
				"encoding": encoding,
				"headers":  r.buildExporterHeaders(basicAuth, "otel-logs", f.TargetDataset, tenantID, config.Spec.Target.Headers, f.Headers),
			}
			pipelines["logs/"+id] = map[string]interface{}{
				"receivers":  []interface{}{"filelog/" + id},
				"processors": []interface{}{"batch"},
				"exporters":  []interface{}{"otlphttp/logs_" + id},
			}
		}
	}

	// Kubeletstats — node, pod, and container CPU/memory/network/filesystem metrics scraped
	// directly from each node's local kubelet at https://${K8S_NODE_NAME}:10250/stats/summary.
	// Runs in the log DaemonSet so each pod talks to its own node (avoids cross-node hops and
	// kubelet TLS SAN mismatches). Requires nodes/stats RBAC (granted in ensureCollectorRBAC).
	if config.Spec.Metrics != nil &&
		config.Spec.Metrics.ClusterMetrics != nil &&
		config.Spec.Metrics.ClusterMetrics.Kubelet != nil &&
		config.Spec.Metrics.ClusterMetrics.Kubelet.Enabled &&
		config.Spec.Metrics.ClusterMetrics.TargetDataset != "" {

		cm := config.Spec.Metrics.ClusterMetrics
		receivers["kubeletstats"] = map[string]interface{}{
			"collection_interval":  "30s",
			"auth_type":            "serviceAccount",
			"endpoint":             "https://${env:K8S_NODE_NAME}:10250",
			"insecure_skip_verify": true,
		}
		exporters["otlphttp/clustermetrics"] = map[string]interface{}{
			"endpoint": endpoint,
			"encoding": encoding,
			"headers":  r.buildExporterHeaders(basicAuth, "otel-metrics", cm.TargetDataset, tenantID, config.Spec.Target.Headers, nil),
		}
		// kubeletstats's /stats/summary response already carries k8s.namespace.name,
		// k8s.pod.name, k8s.container.name, k8s.node.name as resource attributes,
		// so the k8sattributes processor would be redundant here (matches sample YAML).
		pipelines["metrics/kubeletstats"] = map[string]interface{}{
			"receivers":  []interface{}{"kubeletstats"},
			"processors": []interface{}{"batch"},
			"exporters":  []interface{}{"otlphttp/clustermetrics"},
		}
	}

	return map[string]interface{}{
		"receivers":  receivers,
		"processors": processors,
		"exporters":  exporters,
		"service": map[string]interface{}{
			"pipelines": pipelines,
		},
	}, nil
}

// ensureMetricsEventsCollector creates or updates a single Deployment-mode OpenTelemetryCollector CR
// that handles both metrics (k8s_cluster) and events (k8sobjects) as separate pipelines.
// If neither metrics nor events are configured, it deletes any existing collector.
func (r *ParseableConfigReconciler) ensureMetricsEventsCollector(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	gvk := schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(gvk)
	err := r.Get(ctx, client.ObjectKey{Name: metricsEventsCollectorName, Namespace: config.Namespace}, existing)

	metricsEnabled := false
	if config.Spec.Metrics != nil {
		if anyClusterMetricEnabled(config.Spec.Metrics.ClusterMetrics) {
			metricsEnabled = true
		}
		for _, sc := range config.Spec.Metrics.ScrapeConfigs {
			if sc.Name != "" && sc.TargetDataset != "" && sc.Port > 0 {
				metricsEnabled = true
				break
			}
		}
	}
	eventsEnabled := config.Spec.Events != nil && config.Spec.Events.Enabled && config.Spec.Events.TargetDataset != ""

	if !metricsEnabled && !eventsEnabled {
		if err == nil {
			logger.Info("Neither metrics nor events configured, deleting collector")
			if delErr := r.Delete(ctx, existing); delErr != nil && !errors.IsNotFound(delErr) {
				return fmt.Errorf("failed to delete metrics/events collector: %w", delErr)
			}
		}
		return nil
	}

	collectorConfig, cfgErr := r.buildMetricsEventsCollectorConfig(ctx, config, metricsEnabled, eventsEnabled)
	if cfgErr != nil {
		return cfgErr
	}

	spec := map[string]interface{}{
		"mode":   "deployment",
		"config": collectorConfig,
		// Tolerate every taint so the metrics+events Deployment can land on any node —
		// k8sobjects/k8s_cluster only need API access, not specific nodepool placement.
		"tolerations": []interface{}{
			map[string]interface{}{"operator": "Exists"},
		},
	}

	if err == nil {
		existing.Object["spec"] = spec
		if updErr := r.Update(ctx, existing); updErr != nil {
			return fmt.Errorf("failed to update metrics/events collector: %w", updErr)
		}
		logger.Info("Metrics/events collector updated", "metrics", metricsEnabled, "events", eventsEnabled)
		return nil
	}

	if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check metrics/events collector: %w", err)
	}

	logger.Info("Creating metrics/events collector Deployment", "metrics", metricsEnabled, "events", eventsEnabled)
	collector := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "opentelemetry.io/v1beta1",
			"kind":       "OpenTelemetryCollector",
			"metadata": map[string]interface{}{
				"name":      metricsEventsCollectorName,
				"namespace": config.Namespace,
			},
			"spec": spec,
		},
	}

	if crErr := r.Create(ctx, collector); crErr != nil {
		return fmt.Errorf("failed to create metrics/events collector: %w", crErr)
	}

	logger.Info("Metrics/events collector created successfully")
	return nil
}

// buildMetricsEventsCollectorConfig builds a single Deployment-mode collector config that hosts:
//   - one pipeline using the k8s_cluster receiver (cluster-level object state) when ClusterMetrics is enabled;
//   - one pipeline per ScrapeConfigs[] entry using the prometheus receiver with Kubernetes service discovery;
//   - one pipeline using the k8sobjects receiver when Events is enabled.
func (r *ParseableConfigReconciler) buildMetricsEventsCollectorConfig(
	ctx context.Context,
	config *observabilityv1alpha1.ParseableConfig,
	metricsEnabled, eventsEnabled bool,
) (map[string]interface{}, error) {
	_ = metricsEnabled // gating is per-section below

	secret := &corev1.Secret{}
	secretRef := config.Spec.Target.CredentialsSecret
	if err := r.Get(ctx, client.ObjectKey{Name: secretRef.Name, Namespace: secretRef.Namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to read credentials secret %s/%s: %w", secretRef.Namespace, secretRef.Name, err)
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	basicAuth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
	endpoint := strings.TrimRight(config.Spec.Target.Endpoint, "/")
	encoding := resolveOtlpEncoding(config.Spec.Target.Encoding)
	tenantID := config.Spec.Target.GlobalTenantID

	receivers := map[string]interface{}{}
	processors := map[string]interface{}{
		"batch": map[string]interface{}{},
	}
	exporters := map[string]interface{}{}
	pipelines := map[string]interface{}{}

	// Cluster metrics — up to three receivers (k8s_cluster, kubelet /metrics, kube-state-metrics)
	// feed a single shared exporter at ClusterMetrics.TargetDataset.
	if config.Spec.Metrics != nil && anyClusterMetricEnabled(config.Spec.Metrics.ClusterMetrics) {
		cm := config.Spec.Metrics.ClusterMetrics

		var clusterReceivers []interface{}

		if cm.K8sCluster != nil && cm.K8sCluster.Enabled {
			k8sClusterCfg := map[string]interface{}{
				"collection_interval": "30s",
				"auth_type":           "serviceAccount",
			}
			if len(cm.K8sCluster.NodeConditions) > 0 {
				k8sClusterCfg["node_conditions_to_report"] = toInterfaceSlice(cm.K8sCluster.NodeConditions)
			}
			if len(cm.K8sCluster.AllocatableResources) > 0 {
				k8sClusterCfg["allocatable_types_to_report"] = toInterfaceSlice(cm.K8sCluster.AllocatableResources)
			}
			if len(cm.NamespaceSelector.Namespaces) > 0 && cm.NamespaceSelector.Mode == "include" {
				k8sClusterCfg["namespaces"] = toInterfaceSlice(cm.NamespaceSelector.Namespaces)
			}
			receivers["k8s_cluster"] = k8sClusterCfg
			clusterReceivers = append(clusterReceivers, "k8s_cluster")
		}

		// NOTE: Kubelet metrics are sourced via the kubeletstats receiver in the log
		// DaemonSet (see buildLogCollectorConfig). The DaemonSet placement lets each
		// pod hit its own node's kubelet on localhost, which is required because
		// kubelet TLS certs are SAN-bound to the node name. The metrics+events
		// Deployment runs as a single pod and cannot reach every node's kubelet
		// directly, so the kubelet receiver intentionally lives elsewhere.
		_ = cm.Kubelet

		if cm.KubeState != nil && cm.KubeState.Enabled {
			ksNamespaces := cm.KubeState.Namespaces
			if len(ksNamespaces) == 0 {
				ksNamespaces = []string{"kube-system", "kube-state-metrics", "default"}
			}
			receivers["prometheus/kube-state-metrics"] = map[string]interface{}{
				"config": map[string]interface{}{
					"scrape_configs": []interface{}{
						map[string]interface{}{
							"job_name":        "kube-state-metrics",
							"scrape_interval": "30s",
							"kubernetes_sd_configs": []interface{}{
								map[string]interface{}{
									"role": "service",
									"namespaces": map[string]interface{}{
										"names": toInterfaceSlice(ksNamespaces),
									},
								},
							},
							"relabel_configs": []interface{}{
								map[string]interface{}{
									"source_labels": []interface{}{"__meta_kubernetes_service_name"},
									"regex":         ".*kube-state-metrics.*",
									"action":        "keep",
								},
								map[string]interface{}{
									"source_labels": []interface{}{"__meta_kubernetes_service_port_name"},
									"regex":         "metrics|http-metrics",
									"action":        "keep",
								},
							},
						},
					},
				},
			}
			clusterReceivers = append(clusterReceivers, "prometheus/kube-state-metrics")
		}

		exporters["otlphttp/clustermetrics"] = map[string]interface{}{
			"endpoint": endpoint,
			"encoding": encoding,
			"headers":  r.buildExporterHeaders(basicAuth, "otel-metrics", cm.TargetDataset, tenantID, config.Spec.Target.Headers, cm.Headers),
		}
		pipelines["metrics/cluster"] = map[string]interface{}{
			"receivers":  clusterReceivers,
			"processors": []interface{}{"batch"},
			"exporters":  []interface{}{"otlphttp/clustermetrics"},
		}
	}

	// Per-scrape-entry Prometheus pipelines with Kubernetes pod service discovery.
	if config.Spec.Metrics != nil {
		for _, sc := range config.Spec.Metrics.ScrapeConfigs {
			id := sanitizeName(sc.Name)
			if id == "" || sc.TargetDataset == "" || sc.Port <= 0 {
				continue
			}
			metricsPath := sc.URI
			if metricsPath == "" {
				metricsPath = "/metrics"
			}
			if !strings.HasPrefix(metricsPath, "/") {
				metricsPath = "/" + metricsPath
			}

			sdConfig := map[string]interface{}{
				"role": "pod",
			}
			if len(sc.NamespaceSelector.Namespaces) > 0 && sc.NamespaceSelector.Mode == "include" {
				sdConfig["namespaces"] = map[string]interface{}{
					"names": toInterfaceSlice(sc.NamespaceSelector.Namespaces),
				}
			}

			var relabelConfigs []interface{}
			if len(sc.PodSelector) > 0 {
				// Label-based filter: one keep relabel per label key/value.
				for k, v := range sc.PodSelector {
					relabelConfigs = append(relabelConfigs, map[string]interface{}{
						"source_labels": []interface{}{"__meta_kubernetes_pod_label_" + sanitizePromLabel(k)},
						"action":        "keep",
						"regex":         v,
					})
				}
			} else {
				// Port-based filter: keep only pods whose container exposes the configured port.
				relabelConfigs = append(relabelConfigs, map[string]interface{}{
					"source_labels": []interface{}{"__meta_kubernetes_pod_container_port_number"},
					"action":        "keep",
					"regex":         fmt.Sprintf("%d", sc.Port),
				})
			}
			relabelConfigs = append(relabelConfigs, map[string]interface{}{
				"source_labels": []interface{}{"__meta_kubernetes_pod_ip"},
				"action":        "replace",
				"target_label":  "__address__",
				// $$1 → $1 after OTel confmap escape, then Prometheus expands to the captured pod IP.
				"replacement": fmt.Sprintf("$$1:%d", sc.Port),
			})
			if len(sc.NamespaceSelector.Namespaces) > 0 && sc.NamespaceSelector.Mode == "exclude" {
				regex := strings.Join(sc.NamespaceSelector.Namespaces, "|")
				relabelConfigs = append([]interface{}{
					map[string]interface{}{
						"source_labels": []interface{}{"__meta_kubernetes_namespace"},
						"action":        "drop",
						"regex":         regex,
					},
				}, relabelConfigs...)
			}

			receivers["prometheus/"+id] = map[string]interface{}{
				"config": map[string]interface{}{
					"scrape_configs": []interface{}{
						map[string]interface{}{
							"job_name":              id,
							"scrape_interval":       "30s",
							"metrics_path":          metricsPath,
							"kubernetes_sd_configs": []interface{}{sdConfig},
							"relabel_configs":       relabelConfigs,
						},
					},
				},
			}
			exporters["otlphttp/metrics_"+id] = map[string]interface{}{
				"endpoint": endpoint,
				"encoding": encoding,
				"headers":  r.buildExporterHeaders(basicAuth, "otel-metrics", sc.TargetDataset, tenantID, config.Spec.Target.Headers, sc.Headers),
			}
			pipelines["metrics/"+id] = map[string]interface{}{
				"receivers":  []interface{}{"prometheus/" + id},
				"processors": []interface{}{"batch"},
				"exporters":  []interface{}{"otlphttp/metrics_" + id},
			}
		}
	}

	// --- Events pipeline (logs signal) ---
	if eventsEnabled {
		events := config.Spec.Events

		eventsObj := map[string]interface{}{
			"name": "events",
			"mode": "watch",
		}

		// k8sobjects receiver supports namespaces filter for include mode
		if len(events.NamespaceSelector.Namespaces) > 0 && events.NamespaceSelector.Mode == "include" {
			eventsObj["namespaces"] = toInterfaceSlice(events.NamespaceSelector.Namespaces)
		}

		receivers["k8sobjects"] = map[string]interface{}{
			"objects": []interface{}{eventsObj},
		}

		eventsProcessorList := []interface{}{}

		// Exclude mode: drop logs matching excluded namespaces
		if len(events.NamespaceSelector.Namespaces) > 0 && events.NamespaceSelector.Mode == "exclude" {
			var conditions []interface{}
			for _, ns := range events.NamespaceSelector.Namespaces {
				conditions = append(conditions, fmt.Sprintf(`resource.attributes["k8s.namespace.name"] == "%s"`, ns))
			}
			processors["filter/events_ns"] = map[string]interface{}{
				"error_mode":     "ignore",
				"log_conditions": conditions,
			}
			eventsProcessorList = append(eventsProcessorList, "filter/events_ns")
		}

		eventsProcessorList = append(eventsProcessorList, "batch")

		exporters["otlphttp/events"] = map[string]interface{}{
			"endpoint": endpoint,
			"encoding": encoding,
			"headers":  r.buildExporterHeaders(basicAuth, "otel-logs", events.TargetDataset, tenantID, config.Spec.Target.Headers, events.Headers),
		}

		pipelines["logs"] = map[string]interface{}{
			"receivers":  []interface{}{"k8sobjects"},
			"processors": eventsProcessorList,
			"exporters":  []interface{}{"otlphttp/events"},
		}
	}

	collectorConfig := map[string]interface{}{
		"receivers":  receivers,
		"processors": processors,
		"exporters":  exporters,
		"service": map[string]interface{}{
			"pipelines": pipelines,
		},
	}

	return collectorConfig, nil
}

// toInterfaceSlice converts []string to []interface{} for unstructured maps
func toInterfaceSlice(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// resolveOtlpEncoding returns the otlphttp exporter `encoding` value for the
// configured target encoding. Defaults to "json" (Parseable's universally
// supported wire format); "proto" is honored when explicitly set.
func resolveOtlpEncoding(target string) string {
	if target == "proto" {
		return "proto"
	}
	return "json"
}

// resolveOtlpProtocol returns the OTEL_EXPORTER_OTLP_PROTOCOL env-var value
// for SDK instrumentation, matching the target encoding.
func resolveOtlpProtocol(target string) string {
	if target == "proto" {
		return "http/protobuf"
	}
	return "http/json"
}

// sanitizePromLabel converts a Kubernetes label key into the Prometheus relabel form
// (alphanumeric + underscore). Mirrors Prometheus' own label-name sanitization rules.
func sanitizePromLabel(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// anyClusterMetricEnabled reports whether at least one built-in cluster-metrics
// receiver is enabled with a target dataset to ship to.
func anyClusterMetricEnabled(cm *observabilityv1alpha1.ClusterMetricsConfig) bool {
	if cm == nil || cm.TargetDataset == "" {
		return false
	}
	if cm.K8sCluster != nil && cm.K8sCluster.Enabled {
		return true
	}
	if cm.Kubelet != nil && cm.Kubelet.Enabled {
		return true
	}
	if cm.KubeState != nil && cm.KubeState.Enabled {
		return true
	}
	return false
}

// sanitizeName returns a DNS-1123 compliant lowercase identifier derived from s.
// Used for volume names and OTel pipeline/receiver/exporter IDs.
func sanitizeName(s string) string {
	var b strings.Builder
	s = strings.ToLower(s)
	for _, r := range s {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		case r == '-' || r == '_' || r == ' ' || r == '/' || r == '.':
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	for strings.Contains(out, "--") {
		out = strings.ReplaceAll(out, "--", "-")
	}
	return out
}

// buildExporterHeaders returns a merged headers map for collector exporters.
// Merge order: globalHeaders → signalHeaders → built-in headers (Authorization, X-P-Stream, X-P-Log-Source, X-P-Tenant).
// Built-in headers always win.
func (r *ParseableConfigReconciler) buildExporterHeaders(basicAuth, logSource, dataset, tenantID string, globalHeaders, signalHeaders map[string]string) map[string]interface{} {
	headers := map[string]interface{}{}
	// 1. Global headers
	for k, v := range globalHeaders {
		headers[k] = v
	}
	// 2. Signal-level headers (override global)
	for k, v := range signalHeaders {
		headers[k] = v
	}
	// 3. Built-in headers (always win)
	headers["Authorization"] = fmt.Sprintf("Basic %s", basicAuth)
	headers["X-P-Log-Source"] = logSource
	headers["X-P-Stream"] = dataset
	if tenantID != "" {
		headers["X-P-Tenant"] = tenantID
	}
	return headers
}

// buildOtlpHeaders returns a comma-delimited OTEL_EXPORTER_OTLP_HEADERS value for instrumentation env vars.
// Merge order: globalHeaders → signalHeaders → built-in headers.
func (r *ParseableConfigReconciler) buildOtlpHeaders(basicAuth, logSource, dataset, tenantID string, globalHeaders, signalHeaders map[string]string) string {
	merged := map[string]string{}
	for k, v := range globalHeaders {
		merged[k] = v
	}
	for k, v := range signalHeaders {
		merged[k] = v
	}
	// Built-in headers always win
	merged["Authorization"] = fmt.Sprintf("Basic %s", basicAuth)
	merged["X-P-Log-Source"] = logSource
	merged["X-P-Stream"] = dataset
	if tenantID != "" {
		merged["X-P-Tenant"] = tenantID
	}
	var parts []string
	for k, v := range merged {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	return strings.Join(parts, ",")
}

// workloadKey returns a unique key for a workload (kind/namespace/name)
func workloadKey(obj client.Object) string {
	kind := "Deployment"
	if _, ok := obj.(*appsv1.StatefulSet); ok {
		kind = "StatefulSet"
	}
	return fmt.Sprintf("%s/%s/%s", kind, obj.GetNamespace(), obj.GetName())
}

// isWorkloadProcessed checks if a workload should be skipped.
// Skip if:
//   - Already successfully instrumented AND container image hasn't changed
//   - Already tried and failed at the current generation — don't retry same config
//
// Re-process if:
//   - Container image changed since last detection (regardless of success)
//   - Failed (instrumented=false) at an older generation — new config might help
//   - Not in status at all — never processed
func isWorkloadProcessed(config *observabilityv1alpha1.ParseableConfig, obj client.Object, currentImage string) *observabilityv1alpha1.WorkloadInstrumentationStatus {
	kind := "Deployment"
	if _, ok := obj.(*appsv1.StatefulSet); ok {
		kind = "StatefulSet"
	}
	for i := range config.Status.Workloads {
		ws := &config.Status.Workloads[i]
		if ws.Name == obj.GetName() && ws.Namespace == obj.GetNamespace() && ws.Kind == kind {
			// Image changed — re-detect regardless of previous result
			if currentImage != "" && ws.ContainerImage != "" && ws.ContainerImage != currentImage {
				return nil
			}
			// Successfully instrumented — skip
			if ws.Instrumented {
				return ws
			}
			// Failed but already tried at current generation — skip
			if ws.ObservedGeneration == config.Generation {
				return ws
			}
			// Failed at older generation — re-process with new config
			return nil
		}
	}
	return nil
}

// updateWorkloadStatus updates or adds a workload's status entry and persists it
func (r *ParseableConfigReconciler) updateWorkloadStatus(ctx context.Context, config *observabilityv1alpha1.ParseableConfig, obj client.Object, lang string, instrumented bool, containerImage string) {
	logger := log.FromContext(ctx)

	kind := "Deployment"
	if _, ok := obj.(*appsv1.StatefulSet); ok {
		kind = "StatefulSet"
	}

	now := metav1.Now()
	entry := observabilityv1alpha1.WorkloadInstrumentationStatus{
		Name:               obj.GetName(),
		Namespace:          obj.GetNamespace(),
		Kind:               kind,
		DetectedLanguage:   lang,
		Instrumented:       instrumented,
		ContainerImage:     containerImage,
		LastDetectionTime:  &now,
		ObservedGeneration: config.Generation,
	}

	// Re-fetch config to get latest status before updating
	fresh := &observabilityv1alpha1.ParseableConfig{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(config), fresh); err != nil {
		logger.Error(err, "Failed to fetch config for status update")
		return
	}

	// Find and update existing entry, or append
	found := false
	for i := range fresh.Status.Workloads {
		if fresh.Status.Workloads[i].Name == entry.Name &&
			fresh.Status.Workloads[i].Namespace == entry.Namespace &&
			fresh.Status.Workloads[i].Kind == entry.Kind {
			fresh.Status.Workloads[i] = entry
			found = true
			break
		}
	}
	if !found {
		fresh.Status.Workloads = append(fresh.Status.Workloads, entry)
	}

	if err := r.Status().Update(ctx, fresh); err != nil {
		logger.Error(err, "Failed to update workload status", "workload", workloadKey(obj))
	} else {
		logger.Info("Workload status updated", "workload", workloadKey(obj), "language", lang, "instrumented", instrumented)
	}
}

// ensureAnnotations filters workloads by selector and status, then detects
// language via exec and adds the instrumentation annotation (single rollout per workload).
// Workloads already processed (tracked in status) at current generation are skipped.
func (r *ParseableConfigReconciler) ensureAnnotations(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	if config.Spec.Traces == nil || len(config.Spec.Traces.Instrumentation.Languages) == 0 {
		logger.Info("No languages configured, skipping annotation")
		return nil
	}

	// Get namespaces to reconcile
	namespaces, err := r.getReconciledNamespaces(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to get reconciled namespaces: %w", err)
	}

	// Collect all workloads across namespaces
	var allWorkloads []client.Object
	for _, ns := range namespaces {
		deployList := &appsv1.DeploymentList{}
		if err := r.List(ctx, deployList, client.InNamespace(ns)); err != nil {
			logger.Error(err, "Failed to list deployments", "namespace", ns)
			continue
		}
		for i := range deployList.Items {
			allWorkloads = append(allWorkloads, &deployList.Items[i])
		}

		stsList := &appsv1.StatefulSetList{}
		if err := r.List(ctx, stsList, client.InNamespace(ns)); err != nil {
			logger.Error(err, "Failed to list statefulsets", "namespace", ns)
			continue
		}
		for i := range stsList.Items {
			allWorkloads = append(allWorkloads, &stsList.Items[i])
		}
	}

	if len(allWorkloads) == 0 {
		logger.Info("No workloads found in reconciled namespaces")
		return nil
	}

	// Apply workload selector filter
	if config.Spec.Traces.WorkloadSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(&config.Spec.Traces.WorkloadSelector.LabelSelector)
		if err != nil {
			return fmt.Errorf("invalid workloadSelector: %w", err)
		}

		var filtered []client.Object
		for _, obj := range allWorkloads {
			matches := selector.Matches(labels.Set(obj.GetLabels()))
			switch config.Spec.Traces.WorkloadSelector.Mode {
			case "include":
				if matches {
					filtered = append(filtered, obj)
				} else {
					logger.Info("Workload excluded by workloadSelector (include mode)", "name", obj.GetName(), "namespace", obj.GetNamespace())
				}
			case "exclude":
				if !matches {
					filtered = append(filtered, obj)
				} else {
					logger.Info("Workload excluded by workloadSelector (exclude mode)", "name", obj.GetName(), "namespace", obj.GetNamespace())
				}
			default:
				filtered = append(filtered, obj)
			}
		}
		allWorkloads = filtered
		if len(allWorkloads) == 0 {
			logger.Info("No workloads matched workloadSelector")
			return nil
		}
	}

	// Filter out already-processed workloads (using status + image change detection)
	var needsDetection []client.Object
	for _, obj := range allWorkloads {
		w := r.wrapWorkload(obj)
		currentImage := ""
		if w != nil {
			currentImage = w.getContainerImage()
		}
		if ws := isWorkloadProcessed(config, obj, currentImage); ws != nil {
			logger.Info("Workload already processed, skipping",
				"name", obj.GetName(), "namespace", obj.GetNamespace(),
				"language", ws.DetectedLanguage, "instrumented", ws.Instrumented)
			continue
		}
		needsDetection = append(needsDetection, obj)
	}

	if len(needsDetection) == 0 {
		logger.Info("All workloads already processed, nothing to detect")
		return nil
	}

	// Detect language via exec and instrument workloads in parallel
	logger.Info("Starting exec-based language detection", "workloads", len(needsDetection))
	var wg sync.WaitGroup
	for _, obj := range needsDetection {
		wg.Add(1)
		go func(o client.Object) {
			defer wg.Done()
			if err := r.detectLanguage(ctx, config, o); err != nil {
				logger.Error(err, "Language detection failed", "name", o.GetName(), "namespace", o.GetNamespace())
			}
		}(obj)
	}
	wg.Wait()

	return nil
}

// cleanup removes all resources created by the operator in reverse order
func (r *ParseableConfigReconciler) cleanup(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	// Step 0: Clear workload status
	logger.Info("Clearing workload status")
	config.Status.Workloads = nil
	if err := r.Status().Update(ctx, config); err != nil {
		logger.Error(err, "Failed to clear workload status")
	}

	// Step 1: Remove annotations from all workloads
	logger.Info("Removing annotations from workloads")
	namespaces, err := r.getReconciledNamespaces(ctx, config)
	if err != nil {
		logger.Error(err, "Failed to get reconciled namespaces during cleanup")
	} else {
		for _, ns := range namespaces {
			r.removeAnnotationsFromNamespace(ctx, config, ns)
		}
	}

	// Step 2: Delete Instrumentation CR
	logger.Info("Deleting Instrumentation CR", "name", instrumentationName, "namespace", config.Namespace)
	instrumentation := &unstructured.Unstructured{}
	instrumentation.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1alpha1",
		Kind:    "Instrumentation",
	})
	instrumentation.SetName(instrumentationName)
	instrumentation.SetNamespace(config.Namespace)
	if err := r.Delete(ctx, instrumentation); err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete Instrumentation CR")
		}
	} else {
		logger.Info("Instrumentation CR deleted")
	}

	// Step 3: Delete Log Collector CR
	logger.Info("Deleting Log Collector CR", "name", logCollectorName, "namespace", config.Namespace)
	logCollector := &unstructured.Unstructured{}
	logCollector.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	})
	logCollector.SetName(logCollectorName)
	logCollector.SetNamespace(config.Namespace)
	if err := r.Delete(ctx, logCollector); err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete Log Collector CR")
		}
	} else {
		logger.Info("Log Collector CR deleted")
	}

	// Step 4: Delete Metrics/Events Collector CR
	r.deleteCollectorCR(ctx, metricsEventsCollectorName, config.Namespace)

	// Step 5: Delete PAI agent DaemonSet
	logger.Info("Deleting PAI agent DaemonSet")
	agentDS := &appsv1.DaemonSet{}
	if err := r.Get(ctx, client.ObjectKey{Name: paiAgentDaemonSetName, Namespace: config.Namespace}, agentDS); err == nil {
		if err := r.Delete(ctx, agentDS); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete PAI agent DaemonSet")
		} else {
			logger.Info("PAI agent DaemonSet deleted")
		}
	}

	// Step 6: Delete Sidecar Collector CR (legacy)
	logger.Info("Deleting Sidecar Collector CR", "name", sidecarCollectorName, "namespace", config.Namespace)
	collector := &unstructured.Unstructured{}
	collector.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	})
	collector.SetName(sidecarCollectorName)
	collector.SetNamespace(config.Namespace)
	if err := r.Delete(ctx, collector); err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete Sidecar Collector CR")
		}
	} else {
		logger.Info("Sidecar Collector CR deleted")
	}

	return nil
}

// deleteCollectorCR deletes an OpenTelemetryCollector CR by name, ignoring NotFound
func (r *ParseableConfigReconciler) deleteCollectorCR(ctx context.Context, name, namespace string) {
	logger := log.FromContext(ctx)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	})
	obj.SetName(name)
	obj.SetNamespace(namespace)
	if err := r.Delete(ctx, obj); err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete collector CR", "name", name)
		}
	} else {
		logger.Info("Collector CR deleted", "name", name)
	}
}

// removeAnnotationsFromNamespace removes sidecar and instrumentation annotations from all workloads in a namespace
func (r *ParseableConfigReconciler) removeAnnotationsFromNamespace(ctx context.Context, config *observabilityv1alpha1.ParseableConfig, ns string) {
	logger := log.FromContext(ctx)

	// Remove from deployments
	deployList := &appsv1.DeploymentList{}
	if err := r.List(ctx, deployList, client.InNamespace(ns)); err != nil {
		logger.Error(err, "Failed to list deployments for cleanup", "namespace", ns)
	} else {
		for i := range deployList.Items {
			r.removeWorkloadAnnotations(ctx, config, &deployList.Items[i])
		}
	}

	// Remove from statefulsets
	stsList := &appsv1.StatefulSetList{}
	if err := r.List(ctx, stsList, client.InNamespace(ns)); err != nil {
		logger.Error(err, "Failed to list statefulsets for cleanup", "namespace", ns)
	} else {
		for i := range stsList.Items {
			r.removeWorkloadAnnotations(ctx, config, &stsList.Items[i])
		}
	}
}

// removeWorkloadAnnotations removes sidecar and instrumentation annotations from a workload
func (r *ParseableConfigReconciler) removeWorkloadAnnotations(ctx context.Context, config *observabilityv1alpha1.ParseableConfig, obj client.Object) {
	logger := log.FromContext(ctx)

	w := r.wrapWorkload(obj)
	if w == nil {
		return
	}

	annotations := w.getPodTemplateAnnotations()
	if annotations == nil {
		return
	}

	changed := false
	if _, ok := annotations[sidecarAnnotation]; ok {
		delete(annotations, sidecarAnnotation)
		changed = true
	}
	if config.Spec.Traces != nil {
		for _, lang := range config.Spec.Traces.Instrumentation.Languages {
			key := instrumentationAnnotPrefix + lang
			if _, ok := annotations[key]; ok {
				delete(annotations, key)
				changed = true
			}
		}
	}

	if !changed {
		return
	}

	w.setPodTemplateAnnotations(annotations)
	if err := r.Update(ctx, obj); err != nil {
		logger.Error(err, "Failed to remove annotations from workload", "name", obj.GetName(), "namespace", obj.GetNamespace())
	} else {
		logger.Info("Removed annotations from workload", "name", obj.GetName(), "namespace", obj.GetNamespace())
	}
}

// getReconciledNamespaces returns the list of namespaces the operator should reconcile
func (r *ParseableConfigReconciler) getReconciledNamespaces(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) ([]string, error) {
	if config.Spec.Traces == nil {
		return nil, nil
	}

	// Include mode: return only the listed namespaces
	if config.Spec.Traces.NamespaceSelector.Mode == "include" {
		return config.Spec.Traces.NamespaceSelector.Namespaces, nil
	}

	// Exclude mode: return all namespaces except the listed ones
	nsList := &corev1.NamespaceList{}
	if err := r.List(ctx, nsList); err != nil {
		return nil, err
	}

	excludeSet := make(map[string]bool)
	for _, ns := range config.Spec.Traces.NamespaceSelector.Namespaces {
		excludeSet[ns] = true
	}

	var namespaces []string
	for _, ns := range nsList.Items {
		if excludeSet[ns.Name] {
			continue
		}
		namespaces = append(namespaces, ns.Name)
	}

	return namespaces, nil
}

// workload is an interface for deployments and statefulsets
type workload interface {
	client.Object
	getPodTemplateAnnotations() map[string]string
	setPodTemplateAnnotations(map[string]string)
	getPodSelector() labels.Selector
	getContainerImage() string
}

type deploymentWorkload struct {
	*appsv1.Deployment
}

func (d *deploymentWorkload) getPodTemplateAnnotations() map[string]string {
	return d.Spec.Template.Annotations
}

func (d *deploymentWorkload) setPodTemplateAnnotations(a map[string]string) {
	d.Spec.Template.Annotations = a
}

func (d *deploymentWorkload) getPodSelector() labels.Selector {
	sel, _ := labels.Parse(labels.Set(d.Spec.Selector.MatchLabels).String())
	return sel
}

func (d *deploymentWorkload) getContainerImage() string {
	if len(d.Spec.Template.Spec.Containers) > 0 {
		return d.Spec.Template.Spec.Containers[0].Image
	}
	return ""
}

type statefulSetWorkload struct {
	*appsv1.StatefulSet
}

func (s *statefulSetWorkload) getPodTemplateAnnotations() map[string]string {
	return s.Spec.Template.Annotations
}

func (s *statefulSetWorkload) setPodTemplateAnnotations(a map[string]string) {
	s.Spec.Template.Annotations = a
}

func (s *statefulSetWorkload) getPodSelector() labels.Selector {
	sel, _ := labels.Parse(labels.Set(s.Spec.Selector.MatchLabels).String())
	return sel
}

func (s *statefulSetWorkload) getContainerImage() string {
	if len(s.Spec.Template.Spec.Containers) > 0 {
		return s.Spec.Template.Spec.Containers[0].Image
	}
	return ""
}

// detectLanguage detects the workload's language using a multi-phase approach:
//  1. Image heuristics — parse container image name for known language patterns
//  2. Exec-based detection — try multiple running pods, read /proc/1/cmdline and check binaries
//  3. Agent-based detection — for distroless containers, use the PAI agent DaemonSet
//     to read /proc/<pid>/cmdline from the host via cgroup PID lookup
//
// Then adds the correct instrumentation annotation (single rollout).
func (r *ParseableConfigReconciler) detectLanguage(ctx context.Context, config *observabilityv1alpha1.ParseableConfig, obj client.Object) error {
	logger := log.FromContext(ctx)

	w := r.wrapWorkload(obj)
	containerImage := w.getContainerImage()
	languages := config.Spec.Traces.Instrumentation.Languages

	// Phase 1: Image name heuristics — fast, works for distroless too
	lang := detectLanguageByImage(containerImage, languages)
	if lang != "" {
		logger.Info("Language detected via image heuristic", "name", obj.GetName(), "image", containerImage, "language", lang)
	}

	// Phase 2: Exec-based detection — try multiple pods
	if lang == "" {
		pods, err := r.findRunningPods(ctx, w, 3)
		if err != nil || len(pods) == 0 {
			logger.Info("No running pods found, skipping detection", "name", obj.GetName(), "namespace", obj.GetNamespace())
			return nil
		}

		for _, pod := range pods {
			containerName := pod.Spec.Containers[0].Name
			lang = r.detectLanguageByExec(ctx, pod.Name, pod.Namespace, containerName, languages)
			if lang != "" {
				logger.Info("Language detected via exec", "name", obj.GetName(), "pod", pod.Name, "language", lang)
				break
			}
		}

		// Phase 3: Agent-based detection — for distroless containers where exec fails
		if lang == "" {
			for _, pod := range pods {
				lang = r.detectLanguageViaAgent(ctx, pod, languages, config.Namespace)
				if lang != "" {
					logger.Info("Language detected via agent (host /proc)", "name", obj.GetName(), "pod", pod.Name, "language", lang)
					break
				}
			}
		}
	}

	if lang == "" {
		logger.Info("No language detected", "name", obj.GetName(), "namespace", obj.GetNamespace(), "image", containerImage)
		r.updateWorkloadStatus(ctx, config, obj, "", false, containerImage)
		return nil
	}

	// Add instrumentation annotation (single rollout)
	key := instrumentationAnnotPrefix + lang
	for retry := 0; retry < 5; retry++ {
		if err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			return fmt.Errorf("failed to re-fetch workload: %w", err)
		}
		w := r.wrapWorkload(obj)
		annotations := w.getPodTemplateAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations[key] = config.Namespace + "/" + instrumentationName
		w.setPodTemplateAnnotations(annotations)

		if err := r.Update(ctx, obj); err != nil {
			if errors.IsConflict(err) {
				logger.Info("Conflict on update, retrying", "name", obj.GetName(), "retry", retry+1)
				time.Sleep(time.Duration(retry+1) * time.Second)
				continue
			}
			return fmt.Errorf("failed to annotate workload %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
		}
		break
	}

	r.updateWorkloadStatus(ctx, config, obj, lang, true, containerImage)
	return nil
}

// detectLanguageByImage checks the container image name against known language patterns.
// Returns the detected language or empty string if no match.
func detectLanguageByImage(image string, allowedLanguages []string) string {
	if image == "" {
		return ""
	}

	// Strip tag/digest — only check the image name
	imageLower := strings.ToLower(image)
	if idx := strings.LastIndex(imageLower, ":"); idx != -1 {
		imageLower = imageLower[:idx]
	}
	if idx := strings.LastIndex(imageLower, "@"); idx != -1 {
		imageLower = imageLower[:idx]
	}

	allowed := make(map[string]bool, len(allowedLanguages))
	for _, l := range allowedLanguages {
		allowed[l] = true
	}

	for _, h := range imageHeuristics {
		if !allowed[h.language] {
			continue
		}
		for _, pattern := range h.patterns {
			if strings.Contains(imageLower, pattern) {
				return h.language
			}
		}
	}

	return ""
}

// ensurePaiAgent creates or updates the PAI agent DaemonSet that runs on every node.
// The agent mounts the host's /proc and /sys/fs/cgroup read-only so the operator
// can read process cmdlines for distroless containers that don't have a shell.
func (r *ParseableConfigReconciler) ensurePaiAgent(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	// Only needed when traces with instrumentation are configured
	if config.Spec.Traces == nil || len(config.Spec.Traces.Instrumentation.Languages) == 0 {
		// Clean up if it exists
		existing := &appsv1.DaemonSet{}
		if err := r.Get(ctx, client.ObjectKey{Name: paiAgentDaemonSetName, Namespace: config.Namespace}, existing); err == nil {
			logger.Info("No instrumentation configured, deleting PAI agent DaemonSet")
			if err := r.Delete(ctx, existing); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("failed to delete PAI agent DaemonSet: %w", err)
			}
		}
		return nil
	}

	hostPathDirectory := corev1.HostPathDirectory
	privileged := true
	desired := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      paiAgentDaemonSetName,
			Namespace: config.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "pai-agent",
				"app.kubernetes.io/managed-by": "pai",
			},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name": "pai-agent",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name": "pai-agent",
					},
				},
				Spec: corev1.PodSpec{
					HostPID: true,
					Containers: []corev1.Container{
						{
							Name:    "agent",
							Image:   "busybox:stable",
							Command: []string{"sleep", "infinity"},
							SecurityContext: &corev1.SecurityContext{
								Privileged: &privileged,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "host-proc",
									MountPath: "/host/proc",
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "host-proc",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/proc",
									Type: &hostPathDirectory,
								},
							},
						},
					},
				},
			},
		},
	}

	existing := &appsv1.DaemonSet{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		if errors.IsNotFound(err) {
			if err := r.Create(ctx, desired); err != nil {
				return fmt.Errorf("failed to create PAI agent DaemonSet: %w", err)
			}
			logger.Info("PAI agent DaemonSet created")
			return nil
		}
		return err
	}

	// Already exists, nothing to update
	return nil
}

// detectLanguageViaAgent uses the PAI agent DaemonSet to read process cmdlines from the host.
// This works for distroless containers where exec-based detection fails because there is no shell.
// Flow: find agent pod on same node → use hostPID to read /host/proc/*/cmdline → match container PID.
func (r *ParseableConfigReconciler) detectLanguageViaAgent(ctx context.Context, targetPod *corev1.Pod, languages []string, namespace string) string {
	logger := log.FromContext(ctx)

	if targetPod.Status.HostIP == "" {
		return ""
	}

	// Find the target container's ID to look up its PID
	if len(targetPod.Status.ContainerStatuses) == 0 {
		return ""
	}
	containerID := targetPod.Status.ContainerStatuses[0].ContainerID
	if containerID == "" {
		return ""
	}
	// containerID format: containerd://abc123... — extract just the hash
	if idx := strings.LastIndex(containerID, "//"); idx != -1 {
		containerID = containerID[idx+2:]
	}

	// Find the PAI agent pod running on the same node
	agentPod, err := r.findAgentPodOnNode(ctx, targetPod.Spec.NodeName, namespace)
	if err != nil || agentPod == nil {
		logger.Info("No PAI agent pod found on node", "node", targetPod.Spec.NodeName)
		return ""
	}

	// With hostPID, the agent pod can see all host processes at /host/proc.
	// Find the container's PID by grepping cgroup files for the containerID.
	// The script finds PIDs whose cgroup contains the containerID, then reads their cmdline.
	script := fmt.Sprintf(
		`for pid in $(ls /host/proc/ 2>/dev/null | grep -E '^[0-9]+$'); do `+
			`if cat /host/proc/$pid/cgroup 2>/dev/null | grep -q '%s'; then `+
			`cat /host/proc/$pid/cmdline 2>/dev/null | tr '\0' ' '; echo; break; fi; done`,
		containerID[:12], // use first 12 chars of containerID (sufficient for matching)
	)

	stdout, err := r.execInPod(ctx, agentPod.Name, agentPod.Namespace, "agent", []string{"sh", "-c", script})
	if err != nil {
		logger.Info("Failed to exec in agent pod", "error", err, "agentPod", agentPod.Name)
		return ""
	}

	cmdline := strings.ToLower(strings.TrimSpace(stdout))
	if cmdline == "" {
		return ""
	}

	logger.Info("Agent read process cmdline", "targetPod", targetPod.Name, "cmdline", cmdline)

	for _, lang := range languages {
		switch lang {
		case "java":
			if strings.Contains(cmdline, "java") {
				return "java"
			}
		case "nodejs":
			if strings.Contains(cmdline, "node") {
				return "nodejs"
			}
		case "python":
			if strings.Contains(cmdline, "python") {
				return "python"
			}
		case "dotnet":
			if strings.Contains(cmdline, "dotnet") {
				return "dotnet"
			}
		}
	}

	return ""
}

// findAgentPodOnNode returns a running PAI agent pod on the given node
func (r *ParseableConfigReconciler) findAgentPodOnNode(ctx context.Context, nodeName, namespace string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(namespace),
		client.MatchingLabels{"app.kubernetes.io/name": "pai-agent"},
	); err != nil {
		return nil, err
	}
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName == nodeName && pod.Status.Phase == corev1.PodRunning && isPodReady(pod) {
			return pod, nil
		}
	}
	return nil, nil
}

// findRunningPods returns up to maxPods running, ready pods for the given workload.
func (r *ParseableConfigReconciler) findRunningPods(ctx context.Context, w workload, maxPods int) ([]*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(w.GetNamespace()), client.MatchingLabelsSelector{Selector: w.getPodSelector()}); err != nil {
		return nil, err
	}
	var result []*corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.DeletionTimestamp == nil && pod.Status.Phase == corev1.PodRunning && isPodReady(pod) {
			result = append(result, pod)
			if len(result) >= maxPods {
				break
			}
		}
	}
	return result, nil
}

// detectLanguageByExec detects the language runtime by exec-ing into the container.
// First tries reading /proc/1/cmdline, then falls back to checking if language binaries exist.
// Distroless containers (no shell/cat) will return empty — these are typically Go or Native AOT
// builds that don't support auto-instrumentation anyway.
func (r *ParseableConfigReconciler) detectLanguageByExec(ctx context.Context, podName, namespace, containerName string, languages []string) string {
	logger := log.FromContext(ctx)

	// Phase 1: Try reading the main process cmdline
	stdout, err := r.execInPod(ctx, podName, namespace, containerName, []string{"cat", "/proc/1/cmdline"})
	if err == nil {
		cmdline := strings.ToLower(strings.ReplaceAll(stdout, "\x00", " "))
		logger.Info("Read process cmdline", "pod", podName, "cmdline", cmdline)

		for _, lang := range languages {
			switch lang {
			case "java":
				if strings.Contains(cmdline, "java") {
					return "java"
				}
			case "nodejs":
				if strings.Contains(cmdline, "node") {
					return "nodejs"
				}
			case "python":
				if strings.Contains(cmdline, "python") {
					return "python"
				}
			case "dotnet":
				if strings.Contains(cmdline, "dotnet") {
					return "dotnet"
				}
			}
		}
	}

	// Phase 2: Check if language binaries exist
	for _, lang := range languages {
		checks, ok := languageBinaryChecks[lang]
		if !ok {
			continue
		}
		for _, cmd := range checks {
			if _, err := r.execInPod(ctx, podName, namespace, containerName, cmd); err == nil {
				logger.Info("Language binary found", "pod", podName, "language", lang, "binary", cmd[0])
				return lang
			}
		}
	}

	return ""
}

// execInPod executes a command in a container and returns stdout
func (r *ParseableConfigReconciler) execInPod(ctx context.Context, podName, namespace, containerName string, command []string) (string, error) {
	req := r.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(r.RestConfig, "POST", req.URL())
	if err != nil {
		return "", err
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return "", err
	}

	return stdout.String(), nil
}

// wrapWorkload wraps a client.Object into the workload interface
func (r *ParseableConfigReconciler) wrapWorkload(obj client.Object) workload {
	switch v := obj.(type) {
	case *appsv1.Deployment:
		return &deploymentWorkload{v}
	case *appsv1.StatefulSet:
		return &statefulSetWorkload{v}
	}
	return nil
}

// isPodReady checks if all containers in a pod are ready
func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.ContainerStatuses {
		if !c.Ready {
			return false
		}
	}
	return len(pod.Status.ContainerStatuses) > 0
}

// SetupWithManager sets up the controller with the Manager.
func (r *ParseableConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&observabilityv1alpha1.ParseableConfig{}).
		WithEventFilter(GenericPredicates{Client: mgr.GetClient()}).
		Complete(r)
}

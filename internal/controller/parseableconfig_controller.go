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
	"github.com/parseable/pai/internal/helm"
)

const (
	otelOperatorNamespace = "otel-operator"
	otelOperatorRelease   = "opentelemetry-operator"
	otelOperatorRepoName  = "open-telemetry"
	otelOperatorRepoURL   = "https://open-telemetry.github.io/opentelemetry-helm-charts"
	otelOperatorChartRef  = "open-telemetry/opentelemetry-operator"
	instrumentationName   = "pai-instrumentation"

	// kept for cleanup of legacy sidecar resources
	sidecarCollectorName = "pai-sidecar"
	sidecarAnnotation    = "sidecar.opentelemetry.io/inject"

	logCollectorName           = "pai-log-collector"
	metricsEventsCollectorName = "pai-metrics-events-collector"
	collectorClusterRoleName   = "pai-collector"
	paiAgentDaemonSetName      = "pai-agent"

	instrumentationAnnotPrefix = "instrumentation.opentelemetry.io/inject-"
	instrumentationRefValue    = otelOperatorNamespace + "/" + instrumentationName

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

	logger.Info("Reconciling Pai config", "name", config.Name)

	// Step 1: Ensure OpenTelemetry operator is installed
	if err := r.ensureOtelOperator(ctx); err != nil {
		logger.Error(err, "Failed to ensure OpenTelemetry operator")
		return ctrl.Result{}, err
	}

	// Step 2: Ensure Instrumentation CR exists (sends traces directly to Parseable)
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
	if err := r.ensureCollectorRBAC(ctx); err != nil {
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

// ensureOtelOperator checks if the otel operator is installed, installs it if not
func (r *ParseableConfigReconciler) ensureOtelOperator(ctx context.Context) error {
	logger := log.FromContext(ctx)

	helmClient, err := helm.NewClient(otelOperatorNamespace, r.RestConfig)
	if err != nil {
		return fmt.Errorf("failed to create helm client: %w", err)
	}

	// Check if release already exists
	exists, err := helmClient.ReleaseExists(otelOperatorRelease)
	if err != nil {
		return fmt.Errorf("failed to check otel operator release: %w", err)
	}

	if exists {
		logger.Info("OpenTelemetry operator already installed, skipping")
		return nil
	}

	logger.Info("OpenTelemetry operator not found, installing")

	// Add the helm repo
	logger.Info("Adding Helm repository", "name", otelOperatorRepoName, "url", otelOperatorRepoURL)
	if err := helmClient.AddRepository(otelOperatorRepoName, otelOperatorRepoURL); err != nil {
		return fmt.Errorf("failed to add otel helm repo: %w", err)
	}
	logger.Info("Helm repository added successfully", "name", otelOperatorRepoName)

	// Install with required values
	values := map[string]interface{}{
		"manager": map[string]interface{}{
			"collectorImage": map[string]interface{}{
				"repository": "otel/opentelemetry-collector-k8s",
			},
		},
		"admissionWebhooks": map[string]interface{}{
			"certManager": map[string]interface{}{
				"enabled": false,
			},
			"autoGenerateCert": map[string]interface{}{
				"enabled": true,
			},
		},
	}

	logger.Info("Installing Helm chart", "release", otelOperatorRelease, "chart", otelOperatorChartRef, "namespace", otelOperatorNamespace)
	if err := helmClient.InstallChart(ctx, otelOperatorRelease, otelOperatorChartRef, otelOperatorNamespace, values); err != nil {
		return fmt.Errorf("failed to install otel operator: %w", err)
	}

	logger.Info("OpenTelemetry operator installed successfully, waiting for webhook readiness", "release", otelOperatorRelease, "namespace", otelOperatorNamespace)

	// Wait for the OTel operator deployment to be ready (webhook must be serving before we annotate workloads)
	timeout := time.After(3 * time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			return fmt.Errorf("timed out waiting for otel operator to be ready")
		case <-ticker.C:
			deploy := &appsv1.Deployment{}
			if err := r.Get(ctx, client.ObjectKey{Name: "opentelemetry-operator", Namespace: otelOperatorNamespace}, deploy); err != nil {
				logger.Info("Waiting for otel operator deployment...", "error", err.Error())
				continue
			}
			if deploy.Status.ReadyReplicas > 0 && deploy.Status.ReadyReplicas == deploy.Status.Replicas {
				logger.Info("OpenTelemetry operator is ready")
				// Give webhook a few more seconds to register
				time.Sleep(10 * time.Second)
				return nil
			}
			logger.Info("Waiting for otel operator to be ready", "readyReplicas", deploy.Status.ReadyReplicas, "replicas", deploy.Status.Replicas)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ensureCollectorRBAC creates a ClusterRole and ClusterRoleBindings for collector ServiceAccounts.
// The k8s_cluster, k8sobjects, and kubeletstats receivers need permissions to list/watch cluster resources.
func (r *ParseableConfigReconciler) ensureCollectorRBAC(ctx context.Context) error {
	logger := log.FromContext(ctx)

	// Create or update ClusterRole
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: collectorClusterRoleName,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"events", "namespaces", "namespaces/status", "nodes", "nodes/spec", "nodes/stats", "nodes/proxy",
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
			logger.Info("ClusterRole created", "name", collectorClusterRoleName)
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
				Name: "pai-" + sa,
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     collectorClusterRoleName,
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      sa,
					Namespace: otelOperatorNamespace,
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

	tracesStream := config.Spec.Traces.Stream
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
				"value": "http/protobuf",
			},
			map[string]interface{}{
				"name":  "OTEL_EXPORTER_OTLP_HEADERS",
				"value": fmt.Sprintf("Authorization=Basic %s,X-P-Log-Source=otel-traces,X-P-Stream=%s", basicAuth, tracesStream),
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

	err := r.Get(ctx, client.ObjectKey{Name: instrumentationName, Namespace: otelOperatorNamespace}, existing)
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
				"namespace": otelOperatorNamespace,
			},
			"spec": spec,
		},
	}

	if err := r.Create(ctx, instrumentation); err != nil {
		return fmt.Errorf("failed to create Instrumentation CR: %w", err)
	}

	logger.Info("Instrumentation CR created successfully", "name", instrumentationName, "namespace", otelOperatorNamespace)
	return nil
}

// ensureLogCollector creates or updates a DaemonSet-mode OpenTelemetryCollector CR for log collection.
// If logs are not configured, it deletes any existing log collector.
func (r *ParseableConfigReconciler) ensureLogCollector(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	gvk := schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(gvk)
	err := r.Get(ctx, client.ObjectKey{Name: logCollectorName, Namespace: otelOperatorNamespace}, existing)

	// If logs not configured, clean up any existing collector and return
	if config.Spec.Logs == nil || config.Spec.Logs.Stream == "" {
		if err == nil {
			logger.Info("Logs not configured, deleting log collector")
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

	spec := map[string]interface{}{
		"mode":   "daemonset",
		"config": collectorConfig,
		"volumes": []interface{}{
			map[string]interface{}{
				"name": "varlogpods",
				"hostPath": map[string]interface{}{
					"path": "/var/log/pods",
				},
			},
		},
		"volumeMounts": []interface{}{
			map[string]interface{}{
				"name":      "varlogpods",
				"mountPath": "/var/log/pods",
				"readOnly":  true,
			},
		},
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
				"namespace": otelOperatorNamespace,
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

// buildLogCollectorConfig builds the OTel collector pipeline config YAML for log collection.
// It uses the filelog receiver with include/exclude patterns based on the namespace selector,
// k8sattributes processor for metadata enrichment, and otlphttp exporter for Parseable.
func (r *ParseableConfigReconciler) buildLogCollectorConfig(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) (map[string]interface{}, error) {
	logs := config.Spec.Logs

	// Build filelog include/exclude patterns from namespace selector
	var includePatterns, excludePatterns []interface{}

	switch logs.NamespaceSelector.Mode {
	case "include":
		for _, ns := range logs.NamespaceSelector.Namespaces {
			includePatterns = append(includePatterns, fmt.Sprintf("/var/log/pods/%s_*/*/*.log", ns))
		}
	case "exclude":
		includePatterns = []interface{}{"/var/log/pods/*/*/*.log"}
		for _, ns := range logs.NamespaceSelector.Namespaces {
			excludePatterns = append(excludePatterns, fmt.Sprintf("/var/log/pods/%s_*/*/*.log", ns))
		}
	default:
		// No selector — collect from all namespaces
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

	// Read credentials for the exporter
	secret := &corev1.Secret{}
	secretRef := config.Spec.Target.CredentialsSecret
	if err := r.Get(ctx, client.ObjectKey{Name: secretRef.Name, Namespace: secretRef.Namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to read credentials secret %s/%s: %w", secretRef.Namespace, secretRef.Name, err)
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	basicAuth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
	endpoint := strings.TrimRight(config.Spec.Target.Endpoint, "/")

	receivers := map[string]interface{}{
		"filelog": filelogReceiver,
	}
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
	exporters := map[string]interface{}{
		"otlphttp/logs": map[string]interface{}{
			"endpoint": endpoint,
			"headers": map[string]interface{}{
				"Authorization":  fmt.Sprintf("Basic %s", basicAuth),
				"X-P-Log-Source": "otel-logs",
				"X-P-Stream":     logs.Stream,
			},
		},
	}
	pipelines := map[string]interface{}{
		"logs": map[string]interface{}{
			"receivers":  []interface{}{"filelog"},
			"processors": []interface{}{"k8sattributes", "batch"},
			"exporters":  []interface{}{"otlphttp/logs"},
		},
	}

	// Add kubeletstats metrics pipelines — split into node metrics and pod metrics on separate streams.
	if config.Spec.Metrics != nil {
		kubeletstatsAdded := false
		addKubeletstats := func() {
			if !kubeletstatsAdded {
				receivers["kubeletstats"] = map[string]interface{}{
					"collection_interval":  "30s",
					"auth_type":            "serviceAccount",
					"endpoint":             "https://${env:K8S_NODE_NAME}:10250",
					"insecure_skip_verify": true,
				}
				kubeletstatsAdded = true
			}
		}

		// Node metrics pipeline → separate stream
		if config.Spec.Metrics.NodeMetrics != nil && config.Spec.Metrics.NodeMetrics.Stream != "" {
			addKubeletstats()
			exporters["otlphttp/nodemetrics"] = map[string]interface{}{
				"endpoint": endpoint,
				"headers": map[string]interface{}{
					"Authorization":  fmt.Sprintf("Basic %s", basicAuth),
					"X-P-Log-Source": "otel-metrics",
					"X-P-Stream":     config.Spec.Metrics.NodeMetrics.Stream,
				},
			}
			// filter/node_only — keep only k8s.node.* metrics
			processors["filter/node_only"] = map[string]interface{}{
				"error_mode": "ignore",
				"metrics": map[string]interface{}{
					"metric": []interface{}{
						`not IsMatch(name, "^k8s\\.node\\.")`,
					},
				},
			}
			pipelines["metrics/node"] = map[string]interface{}{
				"receivers":  []interface{}{"kubeletstats"},
				"processors": []interface{}{"filter/node_only", "batch"},
				"exporters":  []interface{}{"otlphttp/nodemetrics"},
			}
		}

		// Pod metrics pipeline → separate stream, namespace-filtered
		if config.Spec.Metrics.PodMetrics != nil && config.Spec.Metrics.PodMetrics.Stream != "" {
			addKubeletstats()
			exporters["otlphttp/podmetrics"] = map[string]interface{}{
				"endpoint": endpoint,
				"headers": map[string]interface{}{
					"Authorization":  fmt.Sprintf("Basic %s", basicAuth),
					"X-P-Log-Source": "otel-metrics",
					"X-P-Stream":     config.Spec.Metrics.PodMetrics.Stream,
				},
			}
			// filter/pod_only — drop k8s.node.* metrics, keep pod/container metrics
			processors["filter/pod_only"] = map[string]interface{}{
				"error_mode": "ignore",
				"metrics": map[string]interface{}{
					"metric": []interface{}{
						`IsMatch(name, "^k8s\\.node\\.")`,
					},
				},
			}
			podProcessors := []interface{}{"filter/pod_only"}

			// filter/pod_ns — filter pod metrics to configured namespaces
			podNs := config.Spec.Metrics.PodMetrics.NamespaceSelector
			if len(podNs.Namespaces) > 0 {
				switch podNs.Mode {
				case "include":
					// Drop metrics NOT in the allowed namespaces
					parts := make([]string, 0, len(podNs.Namespaces))
					for _, ns := range podNs.Namespaces {
						parts = append(parts, fmt.Sprintf(`resource.attributes["k8s.namespace.name"] != "%s"`, ns))
					}
					processors["filter/pod_ns"] = map[string]interface{}{
						"error_mode": "ignore",
						"metrics": map[string]interface{}{
							"metric": []interface{}{
								strings.Join(parts, " and "),
							},
						},
					}
					podProcessors = append(podProcessors, "filter/pod_ns")
				case "exclude":
					// Drop metrics IN the excluded namespaces
					var conditions []interface{}
					for _, ns := range podNs.Namespaces {
						conditions = append(conditions, fmt.Sprintf(`resource.attributes["k8s.namespace.name"] == "%s"`, ns))
					}
					processors["filter/pod_ns"] = map[string]interface{}{
						"error_mode": "ignore",
						"metrics": map[string]interface{}{
							"metric": conditions,
						},
					}
					podProcessors = append(podProcessors, "filter/pod_ns")
				}
			}

			podProcessors = append(podProcessors, "batch")
			pipelines["metrics/pod"] = map[string]interface{}{
				"receivers":  []interface{}{"kubeletstats"},
				"processors": podProcessors,
				"exporters":  []interface{}{"otlphttp/podmetrics"},
			}
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
	err := r.Get(ctx, client.ObjectKey{Name: metricsEventsCollectorName, Namespace: otelOperatorNamespace}, existing)

	metricsEnabled := config.Spec.Metrics != nil && config.Spec.Metrics.PodMetrics != nil && config.Spec.Metrics.PodMetrics.Stream != ""
	eventsEnabled := config.Spec.Events != nil && config.Spec.Events.Enabled && config.Spec.Events.Stream != ""

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
				"namespace": otelOperatorNamespace,
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

// buildMetricsEventsCollectorConfig builds a single OTel collector config with:
//   - metrics pipeline: k8s_cluster → filter → batch → otlphttp/metrics
//   - logs pipeline (events): k8sobjects → filter → batch → otlphttp/events
//
// Each pipeline has its own exporter with the correct stream/headers for Parseable.
func (r *ParseableConfigReconciler) buildMetricsEventsCollectorConfig(
	ctx context.Context,
	config *observabilityv1alpha1.ParseableConfig,
	metricsEnabled, eventsEnabled bool,
) (map[string]interface{}, error) {

	// Read credentials (shared by both pipelines)
	secret := &corev1.Secret{}
	secretRef := config.Spec.Target.CredentialsSecret
	if err := r.Get(ctx, client.ObjectKey{Name: secretRef.Name, Namespace: secretRef.Namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to read credentials secret %s/%s: %w", secretRef.Namespace, secretRef.Name, err)
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	basicAuth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
	endpoint := strings.TrimRight(config.Spec.Target.Endpoint, "/")

	receivers := map[string]interface{}{}
	processors := map[string]interface{}{
		"batch": map[string]interface{}{},
	}
	exporters := map[string]interface{}{}
	pipelines := map[string]interface{}{}

	// --- Metrics pipeline (k8s_cluster receiver → podMetrics stream) ---
	if metricsEnabled {
		podMetrics := config.Spec.Metrics.PodMetrics

		k8sClusterCfg := map[string]interface{}{
			"collection_interval": "30s",
		}
		// Use the receiver's native namespaces field to filter at source (include mode only)
		if len(podMetrics.NamespaceSelector.Namespaces) > 0 && podMetrics.NamespaceSelector.Mode == "include" {
			k8sClusterCfg["namespaces"] = toInterfaceSlice(podMetrics.NamespaceSelector.Namespaces)
		}
		receivers["k8s_cluster"] = k8sClusterCfg

		metricsProcessorList := []interface{}{}

		// Filter processor using OTTL metric context — drops metrics not in allowed namespaces
		if len(podMetrics.NamespaceSelector.Namespaces) > 0 && podMetrics.NamespaceSelector.Mode == "include" {
			parts := make([]string, 0, len(podMetrics.NamespaceSelector.Namespaces))
			for _, ns := range podMetrics.NamespaceSelector.Namespaces {
				parts = append(parts, fmt.Sprintf(`resource.attributes["k8s.namespace.name"] != "%s"`, ns))
			}
			condition := strings.Join(parts, " and ")
			processors["filter/metrics_ns"] = map[string]interface{}{
				"error_mode": "ignore",
				"metrics": map[string]interface{}{
					"metric": []interface{}{condition},
				},
			}
			metricsProcessorList = append(metricsProcessorList, "filter/metrics_ns")
		}

		metricsProcessorList = append(metricsProcessorList, "batch")

		exporters["otlphttp/metrics"] = map[string]interface{}{
			"endpoint": endpoint,
			"headers": map[string]interface{}{
				"Authorization":  fmt.Sprintf("Basic %s", basicAuth),
				"X-P-Log-Source": "otel-metrics",
				"X-P-Stream":     podMetrics.Stream,
			},
		}

		pipelines["metrics"] = map[string]interface{}{
			"receivers":  []interface{}{"k8s_cluster"},
			"processors": metricsProcessorList,
			"exporters":  []interface{}{"otlphttp/metrics"},
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
			"headers": map[string]interface{}{
				"Authorization":  fmt.Sprintf("Basic %s", basicAuth),
				"X-P-Log-Source": "otel-logs",
				"X-P-Stream":     events.Stream,
			},
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
	logger.Info("Deleting Instrumentation CR", "name", instrumentationName, "namespace", otelOperatorNamespace)
	instrumentation := &unstructured.Unstructured{}
	instrumentation.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1alpha1",
		Kind:    "Instrumentation",
	})
	instrumentation.SetName(instrumentationName)
	instrumentation.SetNamespace(otelOperatorNamespace)
	if err := r.Delete(ctx, instrumentation); err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete Instrumentation CR")
		}
	} else {
		logger.Info("Instrumentation CR deleted")
	}

	// Step 3: Delete Log Collector CR
	logger.Info("Deleting Log Collector CR", "name", logCollectorName, "namespace", otelOperatorNamespace)
	logCollector := &unstructured.Unstructured{}
	logCollector.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	})
	logCollector.SetName(logCollectorName)
	logCollector.SetNamespace(otelOperatorNamespace)
	if err := r.Delete(ctx, logCollector); err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete Log Collector CR")
		}
	} else {
		logger.Info("Log Collector CR deleted")
	}

	// Step 4: Delete Metrics/Events Collector CR
	r.deleteCollectorCR(ctx, metricsEventsCollectorName)

	// Step 5: Delete PAI agent DaemonSet
	logger.Info("Deleting PAI agent DaemonSet")
	agentDS := &appsv1.DaemonSet{}
	if err := r.Get(ctx, client.ObjectKey{Name: paiAgentDaemonSetName, Namespace: otelOperatorNamespace}, agentDS); err == nil {
		if err := r.Delete(ctx, agentDS); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete PAI agent DaemonSet")
		} else {
			logger.Info("PAI agent DaemonSet deleted")
		}
	}

	// Step 6: Delete Sidecar Collector CR (legacy)
	logger.Info("Deleting Sidecar Collector CR", "name", sidecarCollectorName, "namespace", otelOperatorNamespace)
	collector := &unstructured.Unstructured{}
	collector.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	})
	collector.SetName(sidecarCollectorName)
	collector.SetNamespace(otelOperatorNamespace)
	if err := r.Delete(ctx, collector); err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete Sidecar Collector CR")
		}
	} else {
		logger.Info("Sidecar Collector CR deleted")
	}

	// Step 4: Uninstall OTel operator Helm release
	logger.Info("Uninstalling OpenTelemetry operator Helm release", "release", otelOperatorRelease, "namespace", otelOperatorNamespace)
	helmClient, err := helm.NewClient(otelOperatorNamespace, r.RestConfig)
	if err != nil {
		logger.Error(err, "Failed to create helm client for cleanup")
		return nil
	}

	exists, err := helmClient.ReleaseExists(otelOperatorRelease)
	if err != nil {
		logger.Error(err, "Failed to check helm release during cleanup")
		return nil
	}

	if exists {
		if err := helmClient.UninstallChart(otelOperatorRelease); err != nil {
			logger.Error(err, "Failed to uninstall OTel operator")
		} else {
			logger.Info("OpenTelemetry operator Helm release uninstalled")
		}
	} else {
		logger.Info("OpenTelemetry operator Helm release not found, skipping")
	}

	return nil
}

// deleteCollectorCR deletes an OpenTelemetryCollector CR by name, ignoring NotFound
func (r *ParseableConfigReconciler) deleteCollectorCR(ctx context.Context, name string) {
	logger := log.FromContext(ctx)
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	})
	obj.SetName(name)
	obj.SetNamespace(otelOperatorNamespace)
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
				lang = r.detectLanguageViaAgent(ctx, pod, languages)
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
		annotations[key] = instrumentationRefValue
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
		if err := r.Get(ctx, client.ObjectKey{Name: paiAgentDaemonSetName, Namespace: otelOperatorNamespace}, existing); err == nil {
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
			Namespace: otelOperatorNamespace,
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
func (r *ParseableConfigReconciler) detectLanguageViaAgent(ctx context.Context, targetPod *corev1.Pod, languages []string) string {
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
	agentPod, err := r.findAgentPodOnNode(ctx, targetPod.Spec.NodeName)
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
func (r *ParseableConfigReconciler) findAgentPodOnNode(ctx context.Context, nodeName string) (*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList,
		client.InNamespace(otelOperatorNamespace),
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

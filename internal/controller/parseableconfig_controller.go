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
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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
	sidecarCollectorName  = "pai-sidecar"
	instrumentationName   = "pai-instrumentation"

	sidecarAnnotation          = "sidecar.opentelemetry.io/inject"
	sidecarAnnotationValue     = otelOperatorNamespace + "/" + sidecarCollectorName
	instrumentationAnnotPrefix = "instrumentation.opentelemetry.io/inject-"
	instrumentationRefValue    = otelOperatorNamespace + "/" + instrumentationName

	sidecarMetricsPort         = "8888"
	metricsCheckMetric         = "otelcol_receiver_accepted_spans_total"
	defaultDetectionTimeout    = 1 * time.Minute

	finalizerName = "observability.parseable.com/finalizer"
)

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

	// Step 2: Ensure sidecar collector exists
	if err := r.ensureSidecarCollector(ctx, config); err != nil {
		logger.Error(err, "Failed to ensure sidecar collector")
		return ctrl.Result{}, err
	}

	// Step 3: Ensure Instrumentation CR exists for configured languages
	if err := r.ensureInstrumentation(ctx, config); err != nil {
		logger.Error(err, "Failed to ensure Instrumentation CR")
		return ctrl.Result{}, err
	}

	// Step 4: Annotate deployments and statefulsets in reconciled namespaces
	if err := r.ensureAnnotations(ctx, config); err != nil {
		logger.Error(err, "Failed to ensure annotations")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
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

	logger.Info("OpenTelemetry operator installed successfully", "release", otelOperatorRelease, "namespace", otelOperatorNamespace)
	return nil
}

// ensureSidecarCollector creates the OpenTelemetryCollector sidecar CR if it does not exist
func (r *ParseableConfigReconciler) ensureSidecarCollector(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	// Check if the sidecar collector already exists
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "opentelemetry.io",
		Version: "v1beta1",
		Kind:    "OpenTelemetryCollector",
	})

	err := r.Get(ctx, client.ObjectKey{Name: sidecarCollectorName, Namespace: otelOperatorNamespace}, existing)
	if err == nil {
		logger.Info("Sidecar collector already exists, skipping")
		return nil
	}
	if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check sidecar collector: %w", err)
	}

	// Read credentials secret
	secret := &corev1.Secret{}
	secretRef := config.Spec.Target.CredentialsSecret
	if err := r.Get(ctx, client.ObjectKey{Name: secretRef.Name, Namespace: secretRef.Namespace}, secret); err != nil {
		return fmt.Errorf("failed to read credentials secret %s/%s: %w", secretRef.Namespace, secretRef.Name, err)
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	basicAuth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))

	// Derive TLS insecure from endpoint scheme
	endpoint := config.Spec.Target.Endpoint
	tlsInsecure := !strings.HasPrefix(endpoint, "https")

	// Build collector config
	tracesStream := config.Spec.Target.Streams.Traces
	tracesEndpoint := strings.TrimRight(endpoint, "/") + "/v1/traces"

	// Build the OpenTelemetryCollector CR
	collector := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "opentelemetry.io/v1beta1",
			"kind":       "OpenTelemetryCollector",
			"metadata": map[string]interface{}{
				"name":      sidecarCollectorName,
				"namespace": otelOperatorNamespace,
			},
			"spec": map[string]interface{}{
				"mode": "sidecar",
				"config": map[string]interface{}{
					"receivers": map[string]interface{}{
						"otlp": map[string]interface{}{
							"protocols": map[string]interface{}{
								"grpc": map[string]interface{}{},
								"http": map[string]interface{}{},
							},
						},
					},
					"processors": map[string]interface{}{
						"batch": map[string]interface{}{},
					},
					"exporters": map[string]interface{}{
						"debug": map[string]interface{}{},
						"otlphttp/traces": map[string]interface{}{
							"compression": "gzip",
							"encoding":    "json",
							"headers": map[string]interface{}{
								"Authorization":  fmt.Sprintf("Basic %s", basicAuth),
								"X-P-Log-Source": "otel-traces",
								"X-P-Stream":     tracesStream,
							},
							"traces_endpoint": tracesEndpoint,
							"retry_on_failure": map[string]interface{}{
								"enabled":          true,
								"initial_interval": "5s",
								"max_interval":     "30s",
							},
							"timeout": "30s",
							"tls": map[string]interface{}{
								"insecure": tlsInsecure,
							},
						},
					},
					"service": map[string]interface{}{
						"pipelines": map[string]interface{}{
							"traces": map[string]interface{}{
								"receivers":  []interface{}{"otlp"},
								"processors": []interface{}{"batch"},
								"exporters":  []interface{}{"otlphttp/traces", "debug"},
							},
						},
					},
				},
			},
		},
	}

	if err := r.Create(ctx, collector); err != nil {
		return fmt.Errorf("failed to create sidecar collector: %w", err)
	}

	logger.Info("Sidecar collector created successfully", "name", sidecarCollectorName, "namespace", otelOperatorNamespace)
	return nil
}

// buildInstrumentationSpec builds the Instrumentation spec from the ParseableConfig
func (r *ParseableConfigReconciler) buildInstrumentationSpec(config *observabilityv1alpha1.ParseableConfig) map[string]interface{} {
	spec := map[string]interface{}{
		"exporter": map[string]interface{}{
			"endpoint": "http://localhost:4318",
		},
		"propagators": []interface{}{
			"tracecontext",
			"baggage",
		},
	}

	for _, lang := range config.Spec.Instrumentation.Languages {
		image, ok := languageImages[lang]
		if !ok {
			continue
		}
		spec[lang] = map[string]interface{}{
			"image": image,
		}
	}

	return spec
}

// ensureInstrumentation creates or updates the Instrumentation CR with language sections based on the ParseableConfig
func (r *ParseableConfigReconciler) ensureInstrumentation(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	if len(config.Spec.Instrumentation.Languages) == 0 {
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

	spec := r.buildInstrumentationSpec(config)

	if err == nil {
		// Update existing CR with current languages
		existing.Object["spec"] = spec
		if err := r.Update(ctx, existing); err != nil {
			return fmt.Errorf("failed to update Instrumentation CR: %w", err)
		}
		logger.Info("Instrumentation CR updated", "languages", config.Spec.Instrumentation.Languages)
		return nil
	}

	// Create new
	logger.Info("Creating Instrumentation CR", "languages", config.Spec.Instrumentation.Languages)
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
//   - Already successfully instrumented (any generation) — don't redo what works
//   - Already tried and failed at the current generation — don't retry same config
//
// Re-process if:
//   - Failed (instrumented=false) at an older generation — new config might help
//   - Not in status at all — never processed
func isWorkloadProcessed(config *observabilityv1alpha1.ParseableConfig, obj client.Object) *observabilityv1alpha1.WorkloadInstrumentationStatus {
	kind := "Deployment"
	if _, ok := obj.(*appsv1.StatefulSet); ok {
		kind = "StatefulSet"
	}
	for i := range config.Status.Workloads {
		ws := &config.Status.Workloads[i]
		if ws.Name == obj.GetName() && ws.Namespace == obj.GetNamespace() && ws.Kind == kind {
			// Successfully instrumented — always skip
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
func (r *ParseableConfigReconciler) updateWorkloadStatus(ctx context.Context, config *observabilityv1alpha1.ParseableConfig, obj client.Object, lang string, instrumented bool) {
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

// ensureAnnotations filters workloads by selector and status, then runs
// detectLanguage in parallel for each unprocessed workload.
// detectLanguage sets both sidecar + language annotations in one shot (single rollout per attempt).
// If no language matches, sidecar annotation is removed to revert to original state.
// Workloads already processed (tracked in status) at current generation are skipped.
func (r *ParseableConfigReconciler) ensureAnnotations(ctx context.Context, config *observabilityv1alpha1.ParseableConfig) error {
	logger := log.FromContext(ctx)

	if len(config.Spec.Instrumentation.Languages) == 0 {
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
	if config.Spec.WorkloadSelector != nil {
		selector, err := metav1.LabelSelectorAsSelector(&config.Spec.WorkloadSelector.LabelSelector)
		if err != nil {
			return fmt.Errorf("invalid workloadSelector: %w", err)
		}

		var filtered []client.Object
		for _, obj := range allWorkloads {
			matches := selector.Matches(labels.Set(obj.GetLabels()))
			switch config.Spec.WorkloadSelector.Mode {
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

	// Filter out already-processed workloads (using status)
	var needsDetection []client.Object
	for _, obj := range allWorkloads {
		if ws := isWorkloadProcessed(config, obj); ws != nil {
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

	// Language detection for all workloads in parallel
	// detectLanguage sets both sidecar + language annotations in one shot (single rollout per attempt)
	logger.Info("Starting language detection in parallel", "workloads", len(needsDetection))
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

	// Step 3: Delete Sidecar Collector CR
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
	for _, lang := range config.Spec.Instrumentation.Languages {
		key := instrumentationAnnotPrefix + lang
		if _, ok := annotations[key]; ok {
			delete(annotations, key)
			changed = true
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
	// Include mode: return only the listed namespaces
	if config.Spec.NamespaceSelector.Mode == "include" {
		return config.Spec.NamespaceSelector.Namespaces, nil
	}

	// Exclude mode: return all namespaces except the listed ones
	nsList := &corev1.NamespaceList{}
	if err := r.List(ctx, nsList); err != nil {
		return nil, err
	}

	excludeSet := make(map[string]bool)
	for _, ns := range config.Spec.NamespaceSelector.Namespaces {
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

// detectLanguage tries language annotations one by one on a workload that already has a sidecar.
// For each language, it adds the annotation, waits for rollout, and polls sidecar metrics
// for the configured detectionTimeout. If no language matches, it removes the sidecar annotation.
func (r *ParseableConfigReconciler) detectLanguage(ctx context.Context, config *observabilityv1alpha1.ParseableConfig, obj client.Object) error {
	logger := log.FromContext(ctx)

	detectionTimeout := defaultDetectionTimeout
	if config.Spec.Instrumentation.DetectionTimeout != "" {
		if parsed, err := time.ParseDuration(config.Spec.Instrumentation.DetectionTimeout); err == nil {
			detectionTimeout = parsed
		} else {
			logger.Error(err, "Invalid detectionTimeout, using default", "value", config.Spec.Instrumentation.DetectionTimeout)
		}
	}

	for _, lang := range config.Spec.Instrumentation.Languages {
		key := instrumentationAnnotPrefix + lang

		logger.Info("Trying language annotation", "name", obj.GetName(), "namespace", obj.GetNamespace(), "language", lang)

		// Retry on conflict since multiple goroutines may be updating concurrently
		var existingPods map[string]bool
		var updateErr error
		for retry := 0; retry < 5; retry++ {
			if err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
				return fmt.Errorf("failed to re-fetch workload: %w", err)
			}
			w := r.wrapWorkload(obj)
			annotations := w.getPodTemplateAnnotations()
			if annotations == nil {
				annotations = make(map[string]string)
			}

			for _, l := range config.Spec.Instrumentation.Languages {
				delete(annotations, instrumentationAnnotPrefix+l)
			}
			annotations[sidecarAnnotation] = sidecarAnnotationValue
			annotations[key] = instrumentationRefValue
			w.setPodTemplateAnnotations(annotations)

			existingPods = r.getExistingPodNames(ctx, w)
			updateErr = r.Update(ctx, obj)
			if updateErr == nil {
				break
			}
			if errors.IsConflict(updateErr) {
				logger.Info("Conflict on update, retrying", "name", obj.GetName(), "language", lang, "retry", retry+1)
				time.Sleep(time.Duration(retry+1) * time.Second)
				continue
			}
			return fmt.Errorf("failed to update workload %s/%s with language %s: %w", obj.GetNamespace(), obj.GetName(), lang, updateErr)
		}
		if updateErr != nil {
			return fmt.Errorf("failed to update workload %s/%s with language %s after retries: %w", obj.GetNamespace(), obj.GetName(), lang, updateErr)
		}

		w := r.wrapWorkload(obj)

		// Wait for rollout and get pod name
		podName, err := r.waitForRollout(ctx, w, existingPods)
		if err != nil {
			logger.Error(err, "Rollout wait failed", "name", obj.GetName(), "language", lang)
			continue
		}

		// Poll sidecar metrics for detectionTimeout, checking every 10s
		logger.Info("Waiting for spans", "pod", podName, "namespace", obj.GetNamespace(), "language", lang, "timeout", detectionTimeout)
		deadline := time.After(detectionTimeout)
		ticker := time.NewTicker(10 * time.Second)
		detected := false

		func() {
			defer ticker.Stop()
			for {
				select {
				case <-deadline:
					return
				case <-ticker.C:
					metrics, err := r.fetchSidecarMetrics(ctx, podName, obj.GetNamespace())
					if err != nil {
						logger.Error(err, "Failed to fetch sidecar metrics", "pod", podName, "language", lang)
						continue
					}
					if checkSidecarHasSpans(metrics) {
						detected = true
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		if detected {
			logger.Info("Language detected", "name", obj.GetName(), "namespace", obj.GetNamespace(), "language", lang)
			r.updateWorkloadStatus(ctx, config, obj, lang, true)
			return nil
		}

		logger.Info("No spans detected, trying next language", "name", obj.GetName(), "language", lang)
	}

	// No language matched — remove sidecar annotation to revert to original
	logger.Info("No language matched, reverting to original state", "name", obj.GetName(), "namespace", obj.GetNamespace())
	r.updateWorkloadStatus(ctx, config, obj, "", false)
	for retry := 0; retry < 5; retry++ {
		if err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
			return err
		}
		rw := r.wrapWorkload(obj)
		annotations := rw.getPodTemplateAnnotations()
		if annotations == nil {
			return nil
		}
		delete(annotations, sidecarAnnotation)
		for _, lang := range config.Spec.Instrumentation.Languages {
			delete(annotations, instrumentationAnnotPrefix+lang)
		}
		rw.setPodTemplateAnnotations(annotations)
		err := r.Update(ctx, obj)
		if err == nil {
			logger.Info("Workload reverted to original state", "name", obj.GetName(), "namespace", obj.GetNamespace())
			return nil
		}
		if errors.IsConflict(err) {
			time.Sleep(time.Duration(retry+1) * time.Second)
			continue
		}
		return fmt.Errorf("failed to revert workload to original: %w", err)
	}
	return fmt.Errorf("failed to revert workload %s/%s after retries", obj.GetNamespace(), obj.GetName())
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

// waitForRollout waits for a NEW pod (not in oldPods) to be running with the sidecar
// and returns the pod name. Does not require the pod to be fully ready —
// the detection polling loop handles readiness via metrics checks.
func (r *ParseableConfigReconciler) waitForRollout(ctx context.Context, w workload, oldPods map[string]bool) (string, error) {
	timeout := time.After(5 * time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return "", fmt.Errorf("timed out waiting for rollout of %s/%s", w.GetNamespace(), w.GetName())
		case <-ticker.C:
			podList := &corev1.PodList{}
			if err := r.List(ctx, podList, client.InNamespace(w.GetNamespace()), client.MatchingLabelsSelector{Selector: w.getPodSelector()}); err != nil {
				continue
			}
			for _, pod := range podList.Items {
				if pod.DeletionTimestamp != nil || oldPods[pod.Name] {
					continue
				}
				if pod.Status.Phase == corev1.PodRunning && hasSidecarInitContainer(&pod) {
					return pod.Name, nil
				}
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// hasSidecarInitContainer checks if the pod has the otc-container sidecar init container
func hasSidecarInitContainer(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.InitContainers {
		if c.Name == "otc-container" {
			return true
		}
	}
	return false
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

// getExistingPodNames returns the names of currently existing pods for the workload
func (r *ParseableConfigReconciler) getExistingPodNames(ctx context.Context, w workload) map[string]bool {
	podList := &corev1.PodList{}
	names := make(map[string]bool)
	if err := r.List(ctx, podList, client.InNamespace(w.GetNamespace()), client.MatchingLabelsSelector{Selector: w.getPodSelector()}); err != nil {
		return names
	}
	for _, pod := range podList.Items {
		names[pod.Name] = true
	}
	return names
}


// fetchSidecarMetrics fetches metrics from the sidecar's /metrics endpoint
// Uses the Kubernetes pod proxy API which works both locally and in-cluster
func (r *ParseableConfigReconciler) fetchSidecarMetrics(ctx context.Context, podName, namespace string) (string, error) {
	result := r.Clientset.CoreV1().Pods(namespace).ProxyGet("http", podName, sidecarMetricsPort, "/metrics", nil)
	body, err := result.DoRaw(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to proxy get metrics from pod %s/%s: %w", namespace, podName, err)
	}
	return string(body), nil
}

// checkSidecarHasSpans checks if the sidecar metrics contain accepted spans
func checkSidecarHasSpans(metricsBody string) bool {
	return strings.Contains(metricsBody, metricsCheckMetric)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ParseableConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&observabilityv1alpha1.ParseableConfig{}).
		WithEventFilter(GenericPredicates{Client: mgr.GetClient()}).
		Complete(r)
}

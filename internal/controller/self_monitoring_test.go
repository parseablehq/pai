package controller

import (
	"context"
	"testing"

	observabilityv1alpha1 "github.com/parseable/pai/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func selfMonitoringTestReconciler() *ParseableConfigReconciler {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "parseable-creds", Namespace: "pai-system"},
		Data:       map[string][]byte{"apiKey": []byte("test-key")},
	}
	return &ParseableConfigReconciler{
		Client: fake.NewClientBuilder().WithScheme(k8sscheme.Scheme).WithObjects(secret).Build(),
	}
}

func selfMonitoringTestConfig() *observabilityv1alpha1.ParseableConfig {
	config := traceTestConfig("apiKey")
	config.Spec.Target.GlobalTenantID = "tenant-a"
	config.Spec.Target.Headers = map[string]string{"X-Test": "global"}
	config.Spec.SelfMonitoring = &observabilityv1alpha1.SelfMonitoringConfig{
		TargetDataset: "pai-collector-health",
		Headers:       map[string]string{"X-Test": "self"},
	}
	config.Spec.Logs = &observabilityv1alpha1.LogsConfig{
		PodLogs: &observabilityv1alpha1.PodLogsConfig{Enabled: true, TargetDataset: "pai-logs"},
	}
	config.Spec.Metrics = &observabilityv1alpha1.MetricsConfig{
		ClusterMetrics: &observabilityv1alpha1.ClusterMetricsConfig{
			TargetDataset: "pai-cluster-metrics",
			K8sCluster:    &observabilityv1alpha1.K8sClusterConfig{Enabled: true},
		},
	}
	return config
}

func TestSelfMonitoringPipelineForLogAndMetricsCollectors(t *testing.T) {
	r := selfMonitoringTestReconciler()
	config := selfMonitoringTestConfig()

	builders := []struct {
		name  string
		build func() (map[string]interface{}, error)
	}{
		{logCollectorName, func() (map[string]interface{}, error) {
			return r.buildLogCollectorConfig(context.Background(), config)
		}},
		{metricsEventsCollectorName, func() (map[string]interface{}, error) {
			return r.buildMetricsEventsCollectorConfig(context.Background(), config, true, false)
		}},
	}

	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			collectorConfig, err := builder.build()
			if err != nil {
				t.Fatal(err)
			}
			if !hasSelfMonitoringPipeline(collectorConfig) {
				t.Fatal("self-monitoring pipeline missing")
			}
			receivers := collectorConfig["receivers"].(map[string]interface{})
			receiver := receivers[selfMonitoringReceiver].(map[string]interface{})
			scrape := receiver["config"].(map[string]interface{})["scrape_configs"].([]interface{})[0].(map[string]interface{})
			static := scrape["static_configs"].([]interface{})[0].(map[string]interface{})
			if got := static["targets"].([]interface{})[0]; got != "127.0.0.1:8888" {
				t.Fatalf("scrape target = %v", got)
			}
			labels := static["labels"].(map[string]interface{})
			if labels["pai_collector"] != builder.name || labels["pai_pod"] != "${env:PAI_POD_NAME}" {
				t.Fatalf("self-monitoring labels = %#v", labels)
			}

			exporters := collectorConfig["exporters"].(map[string]interface{})
			exporter := exporters[selfMonitoringExporter].(map[string]interface{})
			headers := exporter["headers"].(map[string]interface{})
			if headers["x-api-key"] != "test-key" || headers["X-P-Stream"] != "pai-collector-health" ||
				headers["X-P-Log-Source"] != "otel-metrics" || headers["X-P-Tenant"] != "tenant-a" ||
				headers["X-Test"] != "self" {
				t.Fatalf("self-monitoring headers = %#v", headers)
			}
		})
	}
}

func TestSelfMonitoringAbsentByDefault(t *testing.T) {
	r := selfMonitoringTestReconciler()
	config := selfMonitoringTestConfig()
	config.Spec.SelfMonitoring = nil

	builders := []func() (map[string]interface{}, error){
		func() (map[string]interface{}, error) { return r.buildLogCollectorConfig(context.Background(), config) },
		func() (map[string]interface{}, error) {
			return r.buildMetricsEventsCollectorConfig(context.Background(), config, true, false)
		},
	}
	for _, build := range builders {
		collectorConfig, err := build()
		if err != nil {
			t.Fatal(err)
		}
		if hasSelfMonitoringPipeline(collectorConfig) {
			t.Fatal("self-monitoring pipeline present without selfMonitoring config")
		}
		if _, found := collectorConfig["receivers"].(map[string]interface{})[selfMonitoringReceiver]; found {
			t.Fatal("self-monitoring receiver present without selfMonitoring config")
		}
	}
}

func TestSelfMonitoringDoesNotChangeTraceCollector(t *testing.T) {
	r := selfMonitoringTestReconciler()
	config := selfMonitoringTestConfig()
	if err := r.ensureTraceCollector(context.Background(), config); err != nil {
		t.Fatal(err)
	}

	collector := &unstructured.Unstructured{}
	collector.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "opentelemetry.io", Version: "v1beta1", Kind: "OpenTelemetryCollector",
	})
	if err := r.Get(context.Background(), client.ObjectKey{Name: traceCollectorName, Namespace: config.Namespace}, collector); err != nil {
		t.Fatal(err)
	}
	spec := collector.Object["spec"].(map[string]interface{})
	if hasSelfMonitoringPipeline(spec["config"]) {
		t.Fatal("self-monitoring pipeline added to trace collector")
	}
	if _, found := spec["env"]; found {
		t.Fatal("self-monitoring pod env added to trace collector")
	}
}

func TestSelfMonitoringDoesNotChangeEventsOnlyCollector(t *testing.T) {
	r := selfMonitoringTestReconciler()
	config := selfMonitoringTestConfig()
	config.Spec.Metrics = nil
	config.Spec.Events = &observabilityv1alpha1.EventsConfig{
		Enabled: true, TargetDataset: "pai-events",
	}
	if err := r.ensureMetricsEventsCollector(context.Background(), config); err != nil {
		t.Fatal(err)
	}

	collector := &unstructured.Unstructured{}
	collector.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "opentelemetry.io", Version: "v1beta1", Kind: "OpenTelemetryCollector",
	})
	if err := r.Get(context.Background(), client.ObjectKey{Name: metricsEventsCollectorName, Namespace: config.Namespace}, collector); err != nil {
		t.Fatal(err)
	}
	spec := collector.Object["spec"].(map[string]interface{})
	if hasSelfMonitoringPipeline(spec["config"]) {
		t.Fatal("self-monitoring pipeline added to events-only collector")
	}
	if _, found := spec["env"]; found {
		t.Fatal("self-monitoring pod env added to events-only collector")
	}
}

func TestSelfMonitoringRemovalFromLogAndMetricsCollectors(t *testing.T) {
	r := selfMonitoringTestReconciler()
	config := selfMonitoringTestConfig()
	config.Spec.Metrics.ClusterMetrics.Kubelet = &observabilityv1alpha1.KubeletConfig{Enabled: true}

	for _, ensure := range []func(context.Context, *observabilityv1alpha1.ParseableConfig) error{
		r.ensureLogCollector,
		r.ensureMetricsEventsCollector,
	} {
		if err := ensure(context.Background(), config); err != nil {
			t.Fatal(err)
		}
	}
	config.Spec.SelfMonitoring = nil
	for _, ensure := range []func(context.Context, *observabilityv1alpha1.ParseableConfig) error{
		r.ensureLogCollector,
		r.ensureMetricsEventsCollector,
	} {
		if err := ensure(context.Background(), config); err != nil {
			t.Fatal(err)
		}
	}

	for _, name := range []string{logCollectorName, metricsEventsCollectorName} {
		collector := &unstructured.Unstructured{}
		collector.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "opentelemetry.io", Version: "v1beta1", Kind: "OpenTelemetryCollector",
		})
		if err := r.Get(context.Background(), client.ObjectKey{Name: name, Namespace: config.Namespace}, collector); err != nil {
			t.Fatal(err)
		}
		spec := collector.Object["spec"].(map[string]interface{})
		if hasSelfMonitoringPipeline(spec["config"]) {
			t.Fatalf("self-monitoring pipeline remained on %s", name)
		}
		if name == metricsEventsCollectorName {
			if _, found := spec["env"]; found {
				t.Fatal("self-monitoring pod env remained on metrics/events collector")
			}
		} else {
			env := spec["env"].([]interface{})
			if len(env) != 1 || env[0].(map[string]interface{})["name"] != "K8S_NODE_NAME" {
				t.Fatalf("unexpected log collector env after removal: %#v", env)
			}
		}
	}
}

func TestSelfMonitoringPodIdentityForEachCollector(t *testing.T) {
	r := selfMonitoringTestReconciler()
	config := selfMonitoringTestConfig()
	config.Spec.Metrics.ClusterMetrics.Kubelet = &observabilityv1alpha1.KubeletConfig{Enabled: true}

	for _, ensure := range []func(context.Context, *observabilityv1alpha1.ParseableConfig) error{
		r.ensureLogCollector,
		r.ensureMetricsEventsCollector,
	} {
		if err := ensure(context.Background(), config); err != nil {
			t.Fatal(err)
		}
	}

	for _, name := range []string{logCollectorName, metricsEventsCollectorName} {
		t.Run(name, func(t *testing.T) {
			collector := &unstructured.Unstructured{}
			collector.SetGroupVersionKind(schema.GroupVersionKind{
				Group: "opentelemetry.io", Version: "v1beta1", Kind: "OpenTelemetryCollector",
			})
			if err := r.Get(context.Background(), client.ObjectKey{Name: name, Namespace: config.Namespace}, collector); err != nil {
				t.Fatal(err)
			}
			spec := collector.Object["spec"].(map[string]interface{})
			if !hasSelfMonitoringPipeline(spec["config"]) {
				t.Fatal("self-monitoring pipeline missing from collector CR")
			}
			env := spec["env"].([]interface{})
			foundPodName := false
			foundNodeName := false
			for _, value := range env {
				item := value.(map[string]interface{})
				if item["name"] == "PAI_POD_NAME" {
					fieldRef := item["valueFrom"].(map[string]interface{})["fieldRef"].(map[string]interface{})
					foundPodName = fieldRef["fieldPath"] == "metadata.name"
				}
				if item["name"] == "K8S_NODE_NAME" {
					foundNodeName = true
				}
			}
			if !foundPodName {
				t.Fatal("collector CR has no pod name env")
			}
			if name == logCollectorName && !foundNodeName {
				t.Fatal("self-monitoring removed kubelet node name env")
			}
		})
	}
}

func hasSelfMonitoringPipeline(collectorConfig interface{}) bool {
	config, ok := collectorConfig.(map[string]interface{})
	if !ok {
		return false
	}
	service, ok := config["service"].(map[string]interface{})
	if !ok {
		return false
	}
	pipelines, ok := service["pipelines"].(map[string]interface{})
	if !ok {
		return false
	}
	_, exists := pipelines[selfMonitoringPipeline]
	return exists
}

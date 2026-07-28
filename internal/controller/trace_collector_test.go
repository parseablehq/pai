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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	observabilityv1alpha1 "github.com/parseable/pai/api/v1alpha1"
)

type updateCountingClient struct {
	client.Client
	updates int
}

func (c *updateCountingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.updates++
	return c.Client.Update(ctx, obj, opts...)
}

func traceTestConfig(authType string) *observabilityv1alpha1.ParseableConfig {
	return &observabilityv1alpha1.ParseableConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "pai-system"},
		Spec: observabilityv1alpha1.ParseableConfigSpec{
			Target: observabilityv1alpha1.TargetConfig{
				Endpoint: "http://parseable.example",
				Encoding: "proto",
				AuthType: authType,
				CredentialsSecret: observabilityv1alpha1.SecretReference{
					Name: "parseable-creds", Namespace: "pai-system",
				},
			},
			Traces: &observabilityv1alpha1.TracesConfig{
				TargetDataset: "pai-traces",
				Instrumentation: observabilityv1alpha1.InstrumentationConfig{
					Languages: []string{"java"},
				},
			},
		},
	}
}

func TestTraceInstrumentationUsesCollectorWithoutParseableCredentials(t *testing.T) {
	r := &ParseableConfigReconciler{}
	spec := r.buildInstrumentationSpec(traceTestConfig("basic"))

	exporter := spec["exporter"].(map[string]interface{})
	wantEndpoint := "http://pai-traces-collector.pai-system:4318"
	if got := exporter["endpoint"]; got != wantEndpoint {
		t.Fatalf("endpoint = %v, want %s", got, wantEndpoint)
	}

	envByName := map[string]string{}
	for _, raw := range spec["env"].([]interface{}) {
		env := raw.(map[string]interface{})
		envByName[env["name"].(string)] = env["value"].(string)
	}
	if got := envByName["OTEL_EXPORTER_OTLP_PROTOCOL"]; got != "http/protobuf" {
		t.Fatalf("protocol = %q, want http/protobuf", got)
	}
	if _, found := envByName["OTEL_EXPORTER_OTLP_HEADERS"]; found {
		t.Fatal("Parseable credentials must not be injected into application pods")
	}
	if envByName["OTEL_LOGS_EXPORTER"] != "none" || envByName["OTEL_METRICS_EXPORTER"] != "none" {
		t.Fatal("application agent logs and metrics exporters must be disabled")
	}
}

func TestTraceCollectorExporterAuthentication(t *testing.T) {
	tests := []struct {
		name       string
		authType   string
		secretData map[string][]byte
		headerName string
		headerWant string
	}{
		{
			name:       "basic",
			secretData: map[string][]byte{"username": []byte("admin"), "password": []byte("admin")},
			headerName: "Authorization",
			headerWant: "Basic YWRtaW46YWRtaW4=",
		},
		{
			name:       "api key",
			authType:   "apiKey",
			secretData: map[string][]byte{"apiKey": []byte("test-key")},
			headerName: "x-api-key",
			headerWant: "test-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "parseable-creds", Namespace: "pai-system"},
				Data:       tt.secretData,
			}
			r := &ParseableConfigReconciler{Client: fake.NewClientBuilder().WithScheme(k8sscheme.Scheme).WithObjects(secret).Build()}
			collectorConfig, err := r.buildTraceCollectorConfig(context.Background(), traceTestConfig(tt.authType))
			if err != nil {
				t.Fatal(err)
			}

			exporters := collectorConfig["exporters"].(map[string]interface{})
			exporter := exporters["otlphttp/traces"].(map[string]interface{})
			headers := exporter["headers"].(map[string]interface{})
			if got := headers[tt.headerName]; got != tt.headerWant {
				t.Fatalf("%s = %v, want %s", tt.headerName, got, tt.headerWant)
			}
			if headers["X-P-Log-Source"] != "otel-traces" || headers["X-P-Stream"] != "pai-traces" {
				t.Fatalf("unexpected Parseable trace headers: %#v", headers)
			}
		})
	}
}

func TestTraceCollectorCRHasOneReplica(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "parseable-creds", Namespace: "pai-system"},
		Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("admin")},
	}
	r := &ParseableConfigReconciler{Client: fake.NewClientBuilder().WithScheme(k8sscheme.Scheme).WithObjects(secret).Build()}
	if err := r.ensureTraceCollector(context.Background(), traceTestConfig("basic")); err != nil {
		t.Fatal(err)
	}

	collector := &unstructured.Unstructured{}
	collector.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "opentelemetry.io", Version: "v1beta1", Kind: "OpenTelemetryCollector",
	})
	if err := r.Get(context.Background(), client.ObjectKey{Name: traceCollectorName, Namespace: "pai-system"}, collector); err != nil {
		t.Fatal(err)
	}
	replicas, found, err := unstructured.NestedInt64(collector.Object, "spec", "replicas")
	if err != nil || !found || replicas != 1 {
		t.Fatalf("replicas = %d, found = %v, err = %v; want 1", replicas, found, err)
	}
}

func TestTraceAnnotationComparisonUsesListedWorkload(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "apps"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					"instrumentation.opentelemetry.io/inject-java": "pai-system/pai-instrumentation-collector-v1",
				}},
			},
		},
	}
	r := &ParseableConfigReconciler{}
	if !r.hasDesiredInstrumentationAnnotation(deployment, "pai-system", "java") {
		t.Fatal("correct Java injection annotation was not recognized")
	}
	if r.hasDesiredInstrumentationAnnotation(deployment, "pai-system", "python") {
		t.Fatal("missing Python injection annotation was incorrectly accepted")
	}
}

func TestTraceWorkloadAutoMigratesLegacyAnnotationOnce(t *testing.T) {
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "apps"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					"instrumentation.opentelemetry.io/inject-java": "pai-system/pai-instrumentation",
				}},
			},
		},
	}
	countingClient := &updateCountingClient{
		Client: fake.NewClientBuilder().WithScheme(k8sscheme.Scheme).WithObjects(deployment).Build(),
	}
	r := &ParseableConfigReconciler{Client: countingClient}

	if err := r.ensureWorkloadInstrumentation(context.Background(), deployment, "pai-system", "java"); err != nil {
		t.Fatal(err)
	}
	if countingClient.updates != 1 {
		t.Fatalf("legacy migration updates = %d, want 1", countingClient.updates)
	}
	if !r.hasDesiredInstrumentationAnnotation(deployment, "pai-system", "java") {
		t.Fatal("legacy Java injection reference was not migrated")
	}
	if err := r.ensureWorkloadInstrumentation(context.Background(), deployment, "pai-system", "java"); err != nil {
		t.Fatal(err)
	}
	if countingClient.updates != 1 {
		t.Fatalf("migrated annotation caused another update; updates = %d", countingClient.updates)
	}
	if err := r.ensureWorkloadInstrumentation(context.Background(), deployment, "other-system", "java"); err != nil {
		t.Fatal(err)
	}
	if countingClient.updates != 2 {
		t.Fatalf("wrong reference updates = %d, want 2 total", countingClient.updates)
	}
}

func TestTracePythonInjectionUpdatesOnlyPythonWorkload(t *testing.T) {
	javaDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "java-api", Namespace: "apps"},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					"instrumentation.opentelemetry.io/inject-java": "pai-system/pai-instrumentation-collector-v1",
				}},
			},
		},
	}
	pythonDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "python-api", Namespace: "apps"},
		Spec:       appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{}},
	}
	countingClient := &updateCountingClient{
		Client: fake.NewClientBuilder().WithScheme(k8sscheme.Scheme).WithObjects(javaDeployment, pythonDeployment).Build(),
	}
	r := &ParseableConfigReconciler{Client: countingClient}

	if !r.hasDesiredInstrumentationAnnotation(javaDeployment, "pai-system", "java") {
		t.Fatal("existing Java workload should remain unchanged")
	}
	if err := r.ensureWorkloadInstrumentation(context.Background(), pythonDeployment, "pai-system", "python"); err != nil {
		t.Fatal(err)
	}
	if countingClient.updates != 1 {
		t.Fatalf("updates = %d, want only the Python workload updated", countingClient.updates)
	}
	if !r.hasDesiredInstrumentationAnnotation(pythonDeployment, "pai-system", "python") {
		t.Fatal("Python injection annotation was not added")
	}
}

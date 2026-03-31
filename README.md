# PAI - Parseable Auto Instrumentation

PAI is a Kubernetes operator that automatically collects logs, metrics, traces, and events from your cluster and sends them to [Parseable](https://parseable.com).

## Prerequisites

- Kubernetes cluster (v1.26+)
- Helm v3
- [OpenTelemetry Operator](https://github.com/open-telemetry/opentelemetry-operator) **v0.99.0+** installed in the cluster

### Compatibility

PAI creates OpenTelemetry CRs and depends on specific API versions:

| Component | API Version | Min OTel Operator Version | Notes |
|-----------|-------------|---------------------------|-------|
| OpenTelemetryCollector | `v1beta1` | v0.99.0 | Introduced in operator v0.99.0 ([changelog](https://github.com/open-telemetry/opentelemetry-operator/releases/tag/v0.99.0)) |
| Instrumentation | `v1alpha1` | v0.43.0 | Stable since early releases |
| Collector image | Operator default | - | Uses `otel/opentelemetry-collector-k8s`, version managed by the operator |

> **Important**: Using an OTel operator version older than v0.99.0 will fail because the `v1beta1` OpenTelemetryCollector CRD will not exist.

### Install OpenTelemetry Operator

PAI requires the OpenTelemetry Operator to be pre-installed. It manages OpenTelemetryCollector and Instrumentation CRDs.

```bash
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
helm install opentelemetry-operator open-telemetry/opentelemetry-operator \
  --namespace otel-operator --create-namespace \
  --set "manager.collectorImage.repository=otel/opentelemetry-collector-k8s" \
  --set admissionWebhooks.certManager.enabled=false \
  --set admissionWebhooks.autoGenerateCert.enabled=true
```

## Installation

### Step 1: Install PAI operator

```bash
helm repo add parseable https://charts.parseable.com/helm-releases
helm repo update
helm install pai parseable/pai -n pai-system --create-namespace
```

Or install from source:

```bash
helm install pai ./helm/pai -n pai-system --create-namespace
```

### Step 2: Create credentials secret

```bash
kubectl create secret generic parseable-creds \
  --from-literal=username=<PARSEABLE_USERNAME> \
  --from-literal=password=<PARSEABLE_PASSWORD> \
  -n pai-system
```

### Step 3: Apply ParseableConfig CR

Create a `ParseableConfig` custom resource to start collecting data:

```yaml
apiVersion: observability.parseable.com/v1alpha1
kind: ParseableConfig
metadata:
  name: production
  namespace: pai-system
spec:
  target:
    endpoint: https://<PARSEABLE_INGESTOR_ENDPOINT>
    credentialsSecret:
      name: parseable-creds
      namespace: pai-system
    globalTenantId: "<TENANT_ID>"        # optional
    headers:                              # optional global headers
      X-P-Environment: "production"

  traces:
    targetDataset: traces
    headers:                              # optional, overrides global headers
      X-P-Telemetry-Type: "traces"
    namespaceSelector:                    # omit to collect from all namespaces
      mode: include
      namespaces:
        - app-namespace
    instrumentation:
      languages:
        - java
        - python
        - nodejs
        - dotnet
      detectionTimeout: "1m"

  logs:
    targetDataset: logs
    headers:
      X-P-Telemetry-Type: "logs"
    namespaceSelector:
      mode: include
      namespaces:
        - app-namespace

  metrics:
    podMetrics:
      targetDataset: pod-metrics
      headers:
        X-P-Telemetry-Type: "metrics"
      namespaceSelector:
        mode: include
        namespaces:
          - app-namespace
    nodeMetrics:
      targetDataset: node-metrics
      headers:
        X-P-Telemetry-Type: "metrics"

  events:
    enabled: true
    targetDataset: events
    headers:
      X-P-Telemetry-Type: "logs"
    namespaceSelector:
      mode: include
      namespaces:
        - app-namespace
```

```bash
kubectl apply -f parseableconfig.yaml
```

## What gets created

Once the `ParseableConfig` CR is applied, PAI automatically creates the following resources in the CR's namespace:

| Resource | Type | Purpose |
|----------|------|---------|
| `pai-log-collector` | OTel Collector (DaemonSet) | Collects logs via filelog receiver, node/pod metrics via kubeletstats |
| `pai-metrics-events-collector` | OTel Collector (Deployment) | Collects pod metrics via k8s_cluster receiver, events via k8sobjects |
| `pai-instrumentation` | Instrumentation CR | Auto-instruments workloads for distributed tracing |
| `pai-agent` | DaemonSet | Detects application languages for distroless containers |
| `<namespace>-pai-collector` | ClusterRole | RBAC for collector service accounts |

## Signals

### Logs
- Collected via the `filelog` receiver on every node
- Enriched with Kubernetes metadata (pod, namespace, node, labels)
- Namespace filtering via `namespaceSelector` (include/exclude mode)

### Traces
- Auto-instrumentation via OpenTelemetry SDK injection (init containers)
- Supported languages: Java, Python, Node.js, .NET
- Language detection order: image heuristics -> exec-based -> host /proc (for distroless)
- Traces are sent directly from instrumented pods to Parseable

### Metrics
- **Pod metrics**: Container CPU, memory, network via `kubeletstats` and `k8s_cluster` receivers
- **Node metrics**: Node-level CPU, memory, disk, network via `kubeletstats` receiver
- Namespace filtering via `namespaceSelector`

### Events
- Kubernetes events collected via `k8sobjects` receiver in watch mode
- Namespace filtering via `namespaceSelector`

## Namespace Selector

Each signal supports a `namespaceSelector` with two modes:

- **include**: Only collect from the listed namespaces
- **exclude**: Collect from all namespaces except the listed ones
- **omitted**: Collect from all namespaces

```yaml
namespaceSelector:
  mode: include
  namespaces:
    - namespace-a
    - namespace-b
```

## Custom Headers

Headers can be set at two levels:

1. **Global** (`spec.target.headers`) - Applied to all signal exporters
2. **Signal-level** (`spec.logs.headers`, `spec.traces.headers`, etc.) - Overrides global headers with the same key

Built-in headers (`Authorization`, `X-P-Stream`, `X-P-Log-Source`, `X-P-Tenant`) always take precedence.

## Pausing Collection

Set `spec.paused: true` to stop all data collection. PAI deletes all collectors, instrumentation, and the agent. Set back to `false` to resume.

```yaml
spec:
  paused: true
```

## Check Status

```bash
kubectl get parseableconfig production -n pai-system -o yaml
```

The `status.workloads` field shows which workloads were instrumented and their detected language.

## Uninstallation

```bash
# Delete the ParseableConfig CR (triggers cleanup of all collectors, instrumentation, annotations)
kubectl delete parseableconfig production -n pai-system

# Uninstall the operator
helm uninstall pai -n pai-system

# Delete the namespace
kubectl delete namespace pai-system
```

For a full cluster cleanup (including OTel operator resources), use the cleanup script:

```bash
curl -sL https://gist.githubusercontent.com/AdheipSingh/0d468e7da14e7b58b66d127c846915d4/raw | bash
```

## Development

```bash
# Run locally
make run

# Build and push image
make docker-build docker-push IMG=<registry>/pai:<tag>

# Generate CRD manifests
make generate && make manifests
```

## License

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

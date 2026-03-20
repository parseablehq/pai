# PAI - Parseable Auto Instrumentation

PAI is a Kubernetes operator that automatically instruments your workloads with OpenTelemetry and sends traces to Parseable.

## Prerequisites

- Kubernetes cluster (1.24+)
- Helm 3
- kubectl

## Install

### 1. Add Helm repo

```bash
helm repo add parseable https://charts.parseable.com
helm repo update
```

### 2. Install PAI operator

```bash
helm install pai parseable/pai -n pai-system --create-namespace
```

### 3. Create credentials secret

Create a secret with your Parseable credentials in the namespace where the operator is running.

```bash
kubectl create secret generic parseable-creds \
  --namespace pai-system \
  --from-literal=username=<your-username> \
  --from-literal=password=<your-password>
```

### 4. Apply ParseableConfig CR

Create a file `parseableconfig.yaml`:

```yaml
apiVersion: observability.parseable.com/v1alpha1
kind: ParseableConfig
metadata:
  name: production
spec:
  namespaceSelector:
    mode: include
    namespaces:
      - default

  target:
    endpoint: https://24c6f9a9-9513-43b2-a83f-bbf3149fc06c-ingestor.workspace.parseable.com
    credentialsSecret:
      name: parseable-creds
      namespace: pai-system
    streams:
      traces: k8s-traces

  instrumentation:
    languages:
      - java
      - python
      - nodejs
      - dotnet
    detectionTimeout: "1m"
```

Apply it:

```bash
kubectl apply -f parseableconfig.yaml
```

## What happens next

1. PAI installs the OpenTelemetry operator automatically
2. Creates a sidecar collector and instrumentation CRs
3. Detects languages for each workload in the target namespace
4. Injects OTel sidecar + auto-instrumentation into matching workloads
5. Traces start flowing to your Parseable instance

## Check status

```bash
kubectl get parseableconfig production -o yaml
```

The `status.workloads` field shows which workloads were instrumented and their detected language.

## Workload Selector (optional)

To instrument only specific workloads, add a `workloadSelector`:

```yaml
spec:
  workloadSelector:
    mode: include
    matchLabels:
      app: my-service
```

## Uninstall

```bash
kubectl delete parseableconfig production
helm uninstall pai -n pai-system
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

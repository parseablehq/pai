#!/bin/bash
# PAI Full Cluster Cleanup Script
# Removes all PAI and OpenTelemetry operator resources from the cluster
# This must be run BEFORE installing PAI to ensure a clean state

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()   { echo -e "${GREEN}[cleanup]${NC} $1"; }
warn()  { echo -e "${YELLOW}[warn]${NC} $1"; }

echo "============================================"
echo "  PAI Full Cluster Cleanup"
echo "============================================"
echo ""

# Step 1: Delete ParseableConfig CRs
log "Removing ParseableConfig CRs..."
kubectl delete parseableconfig --all -A --ignore-not-found 2>/dev/null || true

# Step 2: Delete OpenTelemetryCollector CRs
log "Removing OpenTelemetryCollector CRs..."
kubectl delete opentelemetrycollectors --all -A --ignore-not-found 2>/dev/null || true

# Step 3: Delete Instrumentation CRs
log "Removing Instrumentation CRs..."
kubectl delete instrumentation --all -A --ignore-not-found 2>/dev/null || true

# Step 4: Delete OpAMPBridge CRs
log "Removing OpAMPBridge CRs..."
kubectl delete opampbridges --all -A --ignore-not-found 2>/dev/null || true

# Step 5: Delete pai-agent DaemonSet from all possible namespaces
log "Removing pai-agent DaemonSet..."
for ns in $(kubectl get ns -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    kubectl delete daemonset pai-agent -n "$ns" --ignore-not-found 2>/dev/null || true
done

# Step 6: Uninstall ALL opentelemetry-operator and pai helm releases across all namespaces
log "Uninstalling Helm releases..."
for ns in $(kubectl get ns -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    helm uninstall pai -n "$ns" 2>/dev/null && log "Uninstalled pai from $ns" || true
    helm uninstall opentelemetry-operator -n "$ns" 2>/dev/null && log "Uninstalled opentelemetry-operator from $ns" || true
done

# Step 6b: Delete non-helm OTel operator deployments, services, and service accounts across all namespaces
log "Removing non-helm OpenTelemetry operator resources..."
for ns in $(kubectl get ns -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    kubectl get deployment -n "$ns" -o name 2>/dev/null | grep -iE "otel.*operator|opentelemetry-operator" | while read dep; do
        kubectl delete "$dep" -n "$ns" --ignore-not-found 2>/dev/null && log "Deleted $dep in $ns" || true
    done
    kubectl get service -n "$ns" -o name 2>/dev/null | grep -iE "otel.*operator|opentelemetry-operator" | while read svc; do
        kubectl delete "$svc" -n "$ns" --ignore-not-found 2>/dev/null && log "Deleted $svc in $ns" || true
    done
    kubectl get serviceaccount -n "$ns" -o name 2>/dev/null | grep -iE "otel.*operator|opentelemetry-operator" | while read sa; do
        kubectl delete "$sa" -n "$ns" --ignore-not-found 2>/dev/null && log "Deleted $sa in $ns" || true
    done
    kubectl get roles -n "$ns" -o name 2>/dev/null | grep -iE "otel|opentelemetry" | while read role; do
        kubectl delete "$role" -n "$ns" --ignore-not-found 2>/dev/null && log "Deleted $role in $ns" || true
    done
    kubectl get rolebindings -n "$ns" -o name 2>/dev/null | grep -iE "otel|opentelemetry" | while read rb; do
        kubectl delete "$rb" -n "$ns" --ignore-not-found 2>/dev/null && log "Deleted $rb in $ns" || true
    done
done

# Step 7: Delete ALL webhook configurations matching otel or pai
log "Removing MutatingWebhookConfigurations..."
kubectl get mutatingwebhookconfiguration -o name 2>/dev/null | grep -iE "otel|pai|opentelemetry" | while read wh; do
    kubectl delete "$wh" --ignore-not-found 2>/dev/null && log "Deleted $wh" || true
done

log "Removing ValidatingWebhookConfigurations..."
kubectl get validatingwebhookconfiguration -o name 2>/dev/null | grep -iE "otel|pai|opentelemetry" | while read wh; do
    kubectl delete "$wh" --ignore-not-found 2>/dev/null && log "Deleted $wh" || true
done

# Step 8: Delete ALL ClusterRoleBindings and ClusterRoles matching otel or pai
log "Removing ClusterRoleBindings..."
kubectl get clusterrolebinding -o name 2>/dev/null | grep -iE "otel|pai|opentelemetry" | while read crb; do
    kubectl delete "$crb" --ignore-not-found 2>/dev/null && log "Deleted $crb" || true
done

log "Removing ClusterRoles..."
kubectl get clusterrole -o name 2>/dev/null | grep -iE "otel|pai|opentelemetry" | while read cr; do
    kubectl delete "$cr" --ignore-not-found 2>/dev/null && log "Deleted $cr" || true
done

# Step 9: Delete CRDs
log "Removing CRDs..."
kubectl delete crd parseableconfigs.observability.parseable.com --ignore-not-found 2>/dev/null || true
kubectl delete crd instrumentations.opentelemetry.io --ignore-not-found 2>/dev/null || true
kubectl delete crd opampbridges.opentelemetry.io --ignore-not-found 2>/dev/null || true
kubectl delete crd opentelemetrycollectors.opentelemetry.io --ignore-not-found 2>/dev/null || true

# Step 10: Delete namespaces
log "Removing namespaces..."
for ns in parseable-operator-system pai-system otel-operator my-otel otel; do
    kubectl delete namespace "$ns" --ignore-not-found 2>/dev/null || true
done

echo ""
log "Cleanup complete."

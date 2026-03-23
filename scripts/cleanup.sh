#!/bin/bash
# PAI Full Cluster Cleanup Script
# Removes all PAI and OpenTelemetry operator resources from the cluster

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()   { echo -e "${GREEN}[cleanup]${NC} $1"; }
warn()  { echo -e "${YELLOW}[warn]${NC} $1"; }
err()   { echo -e "${RED}[error]${NC} $1"; }

# Ignore errors for individual deletes since resources may not exist
delete_if_exists() {
    if eval "$1" 2>/dev/null; then
        log "Deleted: $2"
    else
        warn "Not found or already deleted: $2"
    fi
}

echo "============================================"
echo "  PAI Full Cluster Cleanup"
echo "============================================"
echo ""

# Step 1: Delete ParseableConfig CRs (triggers finalizer cleanup)
log "Removing ParseableConfig CRs..."
kubectl delete parseableconfig --all -n parseable-operator-system --ignore-not-found
kubectl delete parseableconfig --all -n pai-system --ignore-not-found
kubectl delete parseableconfig --all -A --ignore-not-found

# Step 2: Delete OpenTelemetryCollector CRs
log "Removing OpenTelemetryCollector CRs..."
kubectl delete opentelemetrycollectors --all -n otel-operator --ignore-not-found
kubectl delete opentelemetrycollectors --all -n my-otel --ignore-not-found
kubectl delete opentelemetrycollectors --all -A --ignore-not-found

# Step 3: Delete Instrumentation CRs
log "Removing Instrumentation CRs..."
kubectl delete instrumentation --all -n otel-operator --ignore-not-found
kubectl delete instrumentation --all -A --ignore-not-found

# Step 4: Delete pai-agent DaemonSet
log "Removing pai-agent DaemonSet..."
kubectl delete daemonset pai-agent -n otel-operator --ignore-not-found
kubectl delete daemonset pai-agent -n pai-system --ignore-not-found

# Step 5: Uninstall Helm releases
log "Uninstalling PAI helm release..."
helm uninstall pai -n parseable-operator-system 2>/dev/null || warn "PAI helm release not found in parseable-operator-system"
helm uninstall pai -n pai-system 2>/dev/null || warn "PAI helm release not found in pai-system"

log "Uninstalling OpenTelemetry Operator helm release (otel-operator)..."
helm uninstall opentelemetry-operator -n otel-operator 2>/dev/null || warn "OTel operator helm release not found in otel-operator"

log "Uninstalling OpenTelemetry Operator helm release (my-otel)..."
helm uninstall opentelemetry-operator -n my-otel 2>/dev/null || warn "OTel operator helm release not found in my-otel"

# Step 6: Delete webhooks
log "Removing MutatingWebhookConfigurations..."
kubectl delete mutatingwebhookconfiguration opentelemetry-operator-mutation --ignore-not-found
kubectl delete mutatingwebhookconfiguration pai-mutating-webhook-configuration --ignore-not-found

log "Removing ValidatingWebhookConfigurations..."
kubectl delete validatingwebhookconfiguration opentelemetry-operator-validation --ignore-not-found
kubectl delete validatingwebhookconfiguration pai-validating-webhook-configuration --ignore-not-found

# Step 7: Delete ClusterRoles and ClusterRoleBindings
log "Removing ClusterRoleBindings..."
for crb in otel-collector pai-admin pai-pai-log-collector-collector pai-pai-metrics-events-collector-collector; do
    kubectl delete clusterrolebinding "$crb" --ignore-not-found
done

log "Removing ClusterRoles..."
for cr in otel-collector pai-collector pai-manager-role pai-metrics-reader pai-proxy-role; do
    kubectl delete clusterrole "$cr" --ignore-not-found
done

# Step 8: Delete CRDs
log "Removing PAI CRDs..."
kubectl delete crd parseableconfigs.observability.parseable.com --ignore-not-found

log "Removing OpenTelemetry CRDs..."
kubectl delete crd instrumentations.opentelemetry.io --ignore-not-found
kubectl delete crd opampbridges.opentelemetry.io --ignore-not-found
kubectl delete crd opentelemetrycollectors.opentelemetry.io --ignore-not-found

# Step 9: Delete namespaces
log "Removing namespaces..."
kubectl delete namespace parseable-operator-system --ignore-not-found
kubectl delete namespace pai-system --ignore-not-found
kubectl delete namespace otel-operator --ignore-not-found
kubectl delete namespace my-otel --ignore-not-found

echo ""
log "Cleanup complete."

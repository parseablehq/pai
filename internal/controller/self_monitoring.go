package controller

import (
	"strings"

	observabilityv1alpha1 "github.com/parseable/pai/api/v1alpha1"
)

const (
	selfMonitoringReceiver = "prometheus/pai_self_monitoring"
	selfMonitoringExporter = "otlphttp/pai_self_monitoring"
	selfMonitoringPipeline = "metrics/pai_self_monitoring"
)

func selfMonitoringEnabled(config *observabilityv1alpha1.ParseableConfig) bool {
	return config.Spec.SelfMonitoring != nil && config.Spec.SelfMonitoring.TargetDataset != ""
}

// addSelfMonitoringPipeline scrapes the collector's own Prometheus endpoint.
// OpenTelemetry Collector exposes internal metrics on localhost:8888 by default.
func (r *ParseableConfigReconciler) addSelfMonitoringPipeline(
	collectorConfig map[string]interface{},
	config *observabilityv1alpha1.ParseableConfig,
	authKey, authValue, collectorName string,
) {
	if !selfMonitoringEnabled(config) {
		return
	}

	self := config.Spec.SelfMonitoring
	receivers := collectorConfig["receivers"].(map[string]interface{})
	exporters := collectorConfig["exporters"].(map[string]interface{})
	service := collectorConfig["service"].(map[string]interface{})
	pipelines := service["pipelines"].(map[string]interface{})

	receivers[selfMonitoringReceiver] = map[string]interface{}{
		"config": map[string]interface{}{
			"scrape_configs": []interface{}{
				map[string]interface{}{
					"job_name":        "pai-collector-self-monitoring",
					"scrape_interval": "15s",
					"static_configs": []interface{}{
						map[string]interface{}{
							"targets": []interface{}{"127.0.0.1:8888"},
							"labels": map[string]interface{}{
								"pai_collector": collectorName,
								"pai_pod":       "${env:PAI_POD_NAME}",
							},
						},
					},
				},
			},
		},
	}
	exporters[selfMonitoringExporter] = map[string]interface{}{
		"endpoint": strings.TrimRight(config.Spec.Target.Endpoint, "/"),
		"encoding": resolveOtlpEncoding(config.Spec.Target.Encoding),
		"headers": r.buildExporterHeaders(
			authKey, authValue, "otel-metrics", self.TargetDataset,
			config.Spec.Target.GlobalTenantID, config.Spec.Target.Headers, self.Headers,
		),
	}
	pipelines[selfMonitoringPipeline] = map[string]interface{}{
		"receivers":  []interface{}{selfMonitoringReceiver},
		"processors": []interface{}{"batch"},
		"exporters":  []interface{}{selfMonitoringExporter},
	}
}

func addSelfMonitoringPodEnv(spec map[string]interface{}) {
	env, _ := spec["env"].([]interface{})
	spec["env"] = append(env, map[string]interface{}{
		"name": "PAI_POD_NAME",
		"valueFrom": map[string]interface{}{
			"fieldRef": map[string]interface{}{"fieldPath": "metadata.name"},
		},
	})
}

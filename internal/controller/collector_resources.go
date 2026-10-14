package controller

import observabilityv1alpha1 "github.com/parseable/pai/api/v1alpha1"

// setCollectorResources writes Kubernetes resource quantities to the OTel
// collector CR spec. An omitted setting leaves the operator's defaults intact.
func setCollectorResources(spec map[string]interface{}, resources *observabilityv1alpha1.CollectorResourceRequirements) {
	if resources == nil {
		return
	}
	values := map[string]interface{}{}
	if len(resources.Requests) > 0 {
		requests := map[string]interface{}{}
		for name, quantity := range resources.Requests {
			requests[string(name)] = quantity.String()
		}
		values["requests"] = requests
	}
	if len(resources.Limits) > 0 {
		limits := map[string]interface{}{}
		for name, quantity := range resources.Limits {
			limits[string(name)] = quantity.String()
		}
		values["limits"] = limits
	}
	if len(values) > 0 {
		spec["resources"] = values
	}
}

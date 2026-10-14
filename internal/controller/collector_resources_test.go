package controller

import (
	"testing"

	observabilityv1alpha1 "github.com/parseable/pai/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestSetCollectorResources(t *testing.T) {
	spec := map[string]interface{}{}
	setCollectorResources(spec, &observabilityv1alpha1.CollectorResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	})

	resources, ok := spec["resources"].(map[string]interface{})
	if !ok {
		t.Fatalf("resources missing: %#v", spec)
	}
	requests := resources["requests"].(map[string]interface{})
	limits := resources["limits"].(map[string]interface{})
	for _, check := range []struct {
		got, want interface{}
	}{
		{requests["cpu"], "100m"},
		{requests["memory"], "128Mi"},
		{limits["cpu"], "500m"},
		{limits["memory"], "512Mi"},
	} {
		if check.got != check.want {
			t.Errorf("resource value = %v, want %v", check.got, check.want)
		}
	}

	empty := map[string]interface{}{}
	setCollectorResources(empty, nil)
	setCollectorResources(empty, &observabilityv1alpha1.CollectorResourceRequirements{})
	if len(empty) != 0 {
		t.Errorf("empty resources should not set spec.resources: %#v", empty)
	}
}

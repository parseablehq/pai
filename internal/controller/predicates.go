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
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	observabilityv1alpha1 "github.com/parseable/pai/api/v1alpha1"
)

// GenericPredicates filters events based on namespaceSelector from ParseableConfig
type GenericPredicates struct {
	predicate.Funcs
	Client client.Client
}

func (p GenericPredicates) Create(e event.CreateEvent) bool {
	return p.ignoreNamespace(e.Object)
}

func (p GenericPredicates) Update(e event.UpdateEvent) bool {
	return p.ignoreNamespace(e.ObjectNew)
}

// ignoreNamespace checks all ParseableConfig CRs for exclude mode
// and filters out namespaces listed in namespaceSelector.namespaces
func (p GenericPredicates) ignoreNamespace(obj client.Object) bool {
	configs := &observabilityv1alpha1.ParseableConfigList{}
	if err := p.Client.List(context.Background(), configs); err != nil {
		return true
	}

	for _, config := range configs.Items {
		if config.Spec.NamespaceSelector.Mode != "exclude" {
			continue
		}
		for _, ns := range config.Spec.NamespaceSelector.Namespaces {
			if obj.GetNamespace() == ns {
				msg := fmt.Sprintf("Pai operator will not reconcile namespace [%s], update namespaceSelector to reconcile", obj.GetNamespace())
				log.Log.Info(msg)
				return false
			}
		}
	}

	return true
}

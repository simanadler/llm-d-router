/*
Copyright 2026 The llm-d Authors.

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

package metrics

import (
	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
)

// The model_name label is populated from the request body, which no
// coordinator config validates against a closed set. Prometheus *Vec types
// never evict label combinations, so an unbounded number of distinct model
// names would grow the time series set without limit and exhaust memory. The
// cap matches EPP's cap for the same reason and for consistency between the
// two components.
const maxModelLabelValues = 1000

var modelLabelLimiter = metricsutil.NewBoundedLabel(maxModelLabelValues)

// boundModel maps a request-derived model name to the label value emitted on
// coordinator metrics. Empty resolves to ModelUnknown before the cap is
// consulted, so a client that never sends "model" cannot exhaust the cap on
// its own.
func boundModel(modelName string) string {
	if modelName == "" {
		return ModelUnknown
	}
	return modelLabelLimiter.Bound(modelName)
}

// Attribution label limiters. tenant_id and workload_id are derived from request
// headers — neither is validated against a closed set. The same cap as model_name
// is used.
//
// Important: these caps bound the number of distinct values admitted *per label
// dimension*, not the total number of Prometheus time series. Series count is
// the cross-product of co-occurring label values, so with three independently
// capped dimensions (model_name, tenant_id, workload_id) the worst-case series
// count on the attributed counters is on the order of 1 000³. All three dimensions
// are client-influenced (two headers + request body), so a single client can drive
// the product without any individual cap being exceeded. These limiters guard
// against unbounded per-dimension value growth; operators of deployments that do
// not enforce attribution headers from a trusted upstream must weigh that
// cross-dimensional risk independently.
const maxAttributionLabelValues = 1000

var (
	tenantIDLabelLimiter   = metricsutil.NewBoundedLabel(maxAttributionLabelValues)
	workloadIDLabelLimiter = metricsutil.NewBoundedLabel(maxAttributionLabelValues)
)

// boundTenantID maps a tenant_id to its capped label value. Empty resolves to
// ModelUnknown before the cap is consulted, matching the behaviour of boundModel.
func boundTenantID(v string) string {
	if v == "" {
		return ModelUnknown
	}
	return tenantIDLabelLimiter.Bound(v)
}

// boundWorkloadID maps a workload_id to its capped label value.
func boundWorkloadID(v string) string {
	if v == "" {
		return ModelUnknown
	}
	return workloadIDLabelLimiter.Bound(v)
}

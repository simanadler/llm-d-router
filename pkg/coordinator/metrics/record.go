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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// IncRequestTotal increments request_total for the given model.
func IncRequestTotal(modelName string) {
	requestTotal.WithLabelValues(boundModel(modelName)).Inc()
}

// IncRequestErrorTotal increments request_error_total for the given model
// and error code.
func IncRequestErrorTotal(modelName, errorCode string) {
	requestErrorTotal.WithLabelValues(boundModel(modelName), errorCode).Inc()
}

// RecordRequestDuration observes an end-to-end request latency.
func RecordRequestDuration(modelName string, d time.Duration) {
	requestDuration.WithLabelValues(boundModel(modelName)).Observe(d.Seconds())
}

// RecordRequestSize observes a request body size in bytes.
func RecordRequestSize(modelName string, bytes int) {
	requestSize.WithLabelValues(boundModel(modelName)).Observe(float64(bytes))
}

// IncRequestRunning increments the in-flight gauge for the given model.
func IncRequestRunning(modelName string) {
	requestRunning.WithLabelValues(boundModel(modelName)).Inc()
}

// DecRequestRunning decrements the in-flight gauge for the given model. It
// must be called exactly once per IncRequestRunning to stay balanced.
func DecRequestRunning(modelName string) {
	requestRunning.WithLabelValues(boundModel(modelName)).Dec()
}

// RecordStepDuration observes the wall-time latency of one pipeline step.
func RecordStepDuration(step string, d time.Duration) {
	stepDuration.WithLabelValues(step).Observe(d.Seconds())
}

// IncStepErrorTotal increments step_errors_total for the given step and
// classified error code.
func IncStepErrorTotal(step, errorCode string) {
	stepErrorTotal.WithLabelValues(step, errorCode).Inc()
}

// IncStepRunning increments the per-step in-flight gauge.
func IncStepRunning(step string) {
	stepRunning.WithLabelValues(step).Inc()
}

// DecStepRunning decrements the per-step in-flight gauge. Balance with
// IncStepRunning.
func DecStepRunning(step string) {
	stepRunning.WithLabelValues(step).Dec()
}

// IncUpstreamRequestTotal increments upstream_request_total for one call.
func IncUpstreamRequestTotal(upstream string) {
	upstreamRequestTotal.WithLabelValues(upstream).Inc()
}

// RecordUpstreamRequestDuration observes the latency of one outbound call.
func RecordUpstreamRequestDuration(upstream string, d time.Duration) {
	upstreamRequestDuration.WithLabelValues(upstream).Observe(d.Seconds())
}

// RecordRequestInputTokens observes the render-derived prompt token count for
// one client request.
func RecordRequestInputTokens(modelName string, tokens int) {
	requestInputTokens.WithLabelValues(boundModel(modelName)).Observe(float64(tokens))
}

// IncExecutionPath increments execution_path_total for the given model and
// path (decode-only, prefill-decode, or encode-prefill-decode).
func IncExecutionPath(modelName, path string) {
	executionPathTotal.WithLabelValues(boundModel(modelName), path).Inc()
}

// IncConditionalDecodeProbes increments conditional_decode_probes_total for
// one probe outcome ("served", "deferred", "error", or "transport_error").
func IncConditionalDecodeProbes(result string) {
	conditionalDecodeProbesTotal.WithLabelValues(result).Inc()
}

// recordAttributed is the shared implementation for RecordAttributedInputTokens
// and RecordAttributedOutputTokens. It builds the label slice based on
// attributionIncludesUserID, which is set once by RegisterAttribution. This
// avoids runtime probing via GetMetricWithLabelValues, whose error-vs-panic
// contract for a label-count mismatch varies across prometheus client versions.
//
// attributionMu is held only long enough to snapshot the three shared vars;
// the counter Add call runs outside the lock to avoid serialising all
// request goroutines on a single mutex.
func recordAttributed(c *prometheus.CounterVec, modelName, tenantID, workloadID, namespace, userID string, tokens int) {
	attributionMu.Lock()
	includeUserID := attributionIncludesUserID
	attributionMu.Unlock()

	lv := []string{
		boundModel(modelName),
		boundTenantID(tenantID),
		boundWorkloadID(workloadID),
		namespace,
	}
	if includeUserID {
		lv = append(lv, userID)
	}
	c.WithLabelValues(lv...).Add(float64(tokens))
}

// RecordAttributedInputTokens increments the attributed input token counter.
// userID is accepted always but is only included in the label map when the
// counter was registered with the user_id dimension.
// No-op when attribution has not been registered (disabled deployment).
func RecordAttributedInputTokens(modelName, tenantID, workloadID, namespace, userID string, tokens int) {
	attributionMu.Lock()
	c := attributedInputTokens
	attributionMu.Unlock()
	if c == nil {
		return
	}
	recordAttributed(c, modelName, tenantID, workloadID, namespace, userID, tokens)
}

// RecordAttributedOutputTokens increments the attributed output token counter.
// See RecordAttributedInputTokens for userID semantics.
func RecordAttributedOutputTokens(modelName, tenantID, workloadID, namespace, userID string, tokens int) {
	attributionMu.Lock()
	c := attributedOutputTokens
	attributionMu.Unlock()
	if c == nil {
		return
	}
	recordAttributed(c, modelName, tenantID, workloadID, namespace, userID, tokens)
}

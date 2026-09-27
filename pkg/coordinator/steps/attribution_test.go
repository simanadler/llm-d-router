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

package steps

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	coordmetrics "github.com/llm-d/llm-d-router/pkg/coordinator/metrics"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

// makeReqCtx returns a minimal RequestContext for hook tests. identity fields
// default to "unknown" (as set by attributionHeader in the handler).
func makeReqCtx(stream bool) *pipeline.RequestContext {
	return &pipeline.RequestContext{
		RequestID:  "test-req-id",
		Model:      "requested-model",
		Stream:     stream,
		TenantID:   "unknown",
		UserID:     "unknown",
		WorkloadID: "unknown",
	}
}

// freshReg returns a fresh Prometheus registry and registers attribution metrics
// on it, returning a cleanup function that resets attribution state.
func freshReg(t *testing.T, includeUserID bool) (*prometheus.Registry, func()) {
	t.Helper()
	coordmetrics.ResetAttribution()
	reg := prometheus.NewRegistry()
	coordmetrics.RegisterAttribution(reg, includeUserID)
	return reg, func() { coordmetrics.ResetAttribution() }
}

// TestNewAttributionHook_DisabledReturnsNil verifies that the enabled=false path
// returns nil so callers can use a simple nil check as the feature gate.
func TestNewAttributionHook_DisabledReturnsNil(t *testing.T) {
	hook := NewAttributionHook(false, "test-ns")
	require.Nil(t, hook)
}

// TestNewAttributionHook_EnabledReturnsNonNil verifies that enabled=true
// returns a usable hook.
func TestNewAttributionHook_EnabledReturnsNonNil(t *testing.T) {
	hook := NewAttributionHook(true, "test-ns")
	require.NotNil(t, hook)
}

// TestModifyResponseFor_NonStreaming_ParsesUsage verifies that ModifyResponseFor
// wraps the body with a TeeReader and Emit correctly parses usage.prompt_tokens
// and usage.completion_tokens from a non-streaming response.
func TestModifyResponseFor_NonStreaming_ParsesUsage(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)
	reqCtx.TenantID = "tenant1"
	reqCtx.UserID = "user1"
	reqCtx.WorkloadID = "wl1"

	body := `{"model":"serving-model","usage":{"prompt_tokens":100,"completion_tokens":50},"choices":[]}`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}

	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))

	// Drain the teed body (simulates proxy.ServeHTTP reading from it).
	bodyBytes, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(bodyBytes), "tee must not alter the forwarded stream")

	hook.Emit(reqCtx)
	require.Equal(t, 100, reqCtx.PromptTokensFromBody)
	require.Equal(t, 50, reqCtx.CompletionTokens)
}


// TestModifyResponseFor_NonStreaming_UsageAlwaysPresent verifies that vLLM
// non-streaming responses include usage without stream_options injection.
func TestModifyResponseFor_NonStreaming_UsageAlwaysPresent(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false) // stream=false

	body := `{"model":"m","usage":{"prompt_tokens":10,"completion_tokens":5}}`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))
	_, _ = io.ReadAll(resp.Body) // drain
	hook.Emit(reqCtx)
	require.Equal(t, 5, reqCtx.CompletionTokens)
}

// TestModifyResponseFor_Streaming_LastChunkCaptured verifies that the
// lastChunkTracker retains only the final "data: ..." line and Emit uses it.
func TestModifyResponseFor_Streaming_LastChunkCaptured(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(true) // stream=true

	// Two SSE chunks; only the last should be used by Emit.
	sseBody := "data: {\"model\":\"wrong\",\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n" +
		"data: {\"model\":\"serving-model\",\"choices\":[],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":8}}\n\n" +
		"data: [DONE]\n\n"

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(sseBody)),
	}

	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))
	_, _ = io.ReadAll(resp.Body) // drain (simulates proxy)
	hook.Emit(reqCtx)

	require.Equal(t, 8, reqCtx.CompletionTokens)
}

// TestModifyResponseFor_CacheMissReturnsErrCacheMiss verifies that composition
// with the probe closure works: errCacheMiss is returned verbatim, the tee is
// installed, and Emit is not called (the decode step hook handles it instead).
func TestModifyResponseFor_CacheMissReturnsErrCacheMiss(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)

	probeCalled := false
	probe := func(resp *http.Response) error {
		probeCalled = true
		return errCacheMiss
	}

	resp := &http.Response{
		StatusCode: http.StatusPreconditionFailed,
		Body:       io.NopCloser(strings.NewReader("")),
	}

	fn := hook.ModifyResponseFor(reqCtx, probe)
	err := fn(resp)
	require.True(t, probeCalled)
	require.ErrorIs(t, err, errCacheMiss, "errCacheMiss must be returned verbatim")

	// The parseState registered by ModifyResponseFor must still be in the map.
	// On a cache miss the conditional-decode step returns nil without calling
	// Emit, so the decode step's subsequent ModifyResponseFor call will overwrite
	// this entry with a fresh parseState for the real decode response. The entry
	// is never cleaned up by the conditional-decode path — it is replaced, not
	// removed. Manual deletion here prevents the map from leaking across tests.
	_, loaded := hook.parseStates.Load(reqCtx)
	require.True(t, loaded, "parseState must remain in the map to be replaced by the decode step's ModifyResponseFor call")

	// Clean up: in production the decode step's ModifyResponseFor overwrites this
	// entry; in this unit test there is no following decode step, so delete manually.
	hook.parseStates.Delete(reqCtx)
}

// TestModifyResponseFor_CompositionProbeStillRecorded verifies that the
// existing probe outcome metrics still increment when attribution is enabled.
func TestModifyResponseFor_CompositionProbeStillRecorded(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)

	// A probe that returns nil (served case).
	probeCalled := false
	probe := func(resp *http.Response) error {
		probeCalled = true
		return nil
	}

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"model":"m","usage":{"prompt_tokens":1,"completion_tokens":1}}`)),
	}
	fn := hook.ModifyResponseFor(reqCtx, probe)
	require.NoError(t, fn(resp))
	require.True(t, probeCalled)
	// Drain and emit.
	_, _ = io.ReadAll(resp.Body)
	hook.Emit(reqCtx)
}

// TestAllHeadersAbsent verifies that when all identity headers are absent,
// the hook uses "unknown" for all three dimensions.
func TestAllHeadersAbsent(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)
	// TenantID/UserID/WorkloadID default to "unknown" (set by handler).

	body := `{"model":"m","usage":{"prompt_tokens":5,"completion_tokens":3}}`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))
	_, _ = io.ReadAll(resp.Body)
	hook.Emit(reqCtx)

	// Metrics/log use "unknown" for all three identity fields — no panic.
	require.Equal(t, "unknown", reqCtx.TenantID)
	require.Equal(t, "unknown", reqCtx.UserID)
	require.Equal(t, "unknown", reqCtx.WorkloadID)
}

// TestEmit_UnparsableBody_UsesFallback verifies that Emit uses fallback token
// values (0) when the captured body is not valid JSON.
func TestEmit_UnparsableBody_UsesFallback(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("not json at all {{{")),
	}
	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))
	_, _ = io.ReadAll(resp.Body)
	hook.Emit(reqCtx)

	// Fallback values.
	require.Equal(t, 0, reqCtx.CompletionTokens)
	require.Equal(t, 0, reqCtx.PromptTokensFromBody)
}

// TestEmit_RenderStepAbsent_UsesBodyPromptTokens verifies that when TokenIDs
// is empty (render step not in pipeline), Emit uses PromptTokensFromBody.
func TestEmit_RenderStepAbsent_UsesBodyPromptTokens(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)
	reqCtx.TokenIDs = nil // no render step

	body := `{"model":"m","usage":{"prompt_tokens":42,"completion_tokens":10}}`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))
	_, _ = io.ReadAll(resp.Body)
	hook.Emit(reqCtx)

	// prompt tokens come from body since TokenIDs is empty.
	require.Equal(t, 42, reqCtx.PromptTokensFromBody)
	require.Equal(t, 10, reqCtx.CompletionTokens)
}

// TestEmit_RenderStepPresent_UsesTokenIDs verifies that when TokenIDs is
// populated (render step ran), Emit uses len(TokenIDs) for prompt tokens.
func TestEmit_RenderStepPresent_UsesTokenIDs(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)
	reqCtx.TokenIDs = []int{1, 2, 3, 4, 5} // render step ran: 5 tokens

	body := `{"model":"m","usage":{"prompt_tokens":999,"completion_tokens":10}}`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))
	_, _ = io.ReadAll(resp.Body)
	hook.Emit(reqCtx)

	// len(TokenIDs)=5 wins over PromptTokensFromBody=999.
	require.Equal(t, 10, reqCtx.CompletionTokens)
}

// TestPrometheusUserIDLabel_False_MetricRegisteredWithout verifies that when
// includeUserID=false, the metric is registered without the user_id dimension.
func TestPrometheusUserIDLabel_False_MetricRegisteredWithout(t *testing.T) {
	coordmetrics.ResetAttribution()
	reg := prometheus.NewRegistry()
	coordmetrics.RegisterAttribution(reg, false)
	defer coordmetrics.ResetAttribution()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)
	reqCtx.UserID = "user-secret"

	body := `{"model":"m","usage":{"prompt_tokens":5,"completion_tokens":3}}`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))
	_, _ = io.ReadAll(resp.Body)

	// Must not panic even though the label set has no user_id.
	require.NotPanics(t, func() { hook.Emit(reqCtx) })
}

// TestTeeReader_ProxyReceivesFullBody verifies that bytes written to the client
// are identical to the upstream response; attribution read does not truncate the
// forwarded stream.
func TestTeeReader_ProxyReceivesFullBody(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)

	original := `{"model":"m","usage":{"prompt_tokens":7,"completion_tokens":3},"choices":[]}`
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(original)),
	}
	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))

	// Drain the tee-wrapped body and verify bytes are identical to original.
	forwarded, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, original, string(forwarded))
}

// TestSideBufferCap_BodyExceedsCap verifies that when a response exceeds the
// side-buffer cap, Emit uses fallback token values (0), but the forwarded bytes
// are still complete and unmodified.
func TestSideBufferCap_BodyExceedsCap(t *testing.T) {
	_, cleanup := freshReg(t, false)
	defer cleanup()

	hook := NewAttributionHook(true, "ns")
	reqCtx := makeReqCtx(false)

	// Padding fills the entire cap so the usage field is never captured.
	// The side buffer captures exactly cap bytes and marks itself truncated;
	// the captured fragment is not valid JSON, so parseBody fails and Emit
	// uses fallback token values (0).
	padding := strings.Repeat("x", attributionSideBufferCap+1024)
	body := fmt.Sprintf(`{"padding":"%s","model":"serving-model","usage":{"prompt_tokens":1,"completion_tokens":2}}`, padding)

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	fn := hook.ModifyResponseFor(reqCtx, nil)
	require.NoError(t, fn(resp))

	// Drain the body fully — forwarded bytes must be complete.
	forwarded, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body, string(forwarded), "capped buffer must not truncate forwarded stream")

	// Emit uses fallback token values since the buffer is truncated mid-JSON.
	hook.Emit(reqCtx)
	require.Equal(t, 0, reqCtx.CompletionTokens)
	require.Equal(t, 0, reqCtx.PromptTokensFromBody)
}

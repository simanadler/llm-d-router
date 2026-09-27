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

package pipeline_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	"github.com/llm-d/llm-d-router/pkg/coordinator/config"
	"github.com/llm-d/llm-d-router/pkg/coordinator/connectors/ec"
	"github.com/llm-d/llm-d-router/pkg/coordinator/connectors/kv"
	"github.com/llm-d/llm-d-router/pkg/coordinator/gateway"
	coordmetrics "github.com/llm-d/llm-d-router/pkg/coordinator/metrics"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
	coordsteps "github.com/llm-d/llm-d-router/pkg/coordinator/steps"
)

func TestFullPipeline_AllConnectorCombinations(t *testing.T) {
	cases := []struct {
		kvConnector     string
		ecConnector     string
		wantECInPrefill bool // ec_transfer_params should be present in prefill body
	}{
		{kv.NIXL, ec.NIXL, true},
		{kv.NIXL, ec.SharedStorage, false},
		{kv.SharedStorage, ec.NIXL, true},
		{kv.SharedStorage, ec.SharedStorage, false},
	}

	for _, tc := range cases {
		t.Run(tc.kvConnector+"+"+tc.ecConnector, func(t *testing.T) {
			renderServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"token_ids": []int{1, 32000, 32000, 32000, 2345, 6789},
					"features": map[string]any{
						"mm_hashes":       map[string][]string{coordsteps.ModalityImage: {"vllm-hash-img0"}},
						"mm_placeholders": map[string][]any{coordsteps.ModalityImage: {map[string]any{"offset": 1, "length": 3}}},
						"kwargs_data":     map[string][]string{coordsteps.ModalityImage: {"dGVuc29yLWRhdGE="}},
					},
				})
			}))
			defer renderServer.Close()

			var mu sync.Mutex
			var capturedPrefillBody map[string]any

			gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				phase := r.Header.Get(gateway.EPPProfileHeader)
				switch phase {
				case gateway.PhaseEncode:
					body, _ := io.ReadAll(r.Body)
					var parsed map[string]any
					_ = json.Unmarshal(body, &parsed)
					// Generate format: features at top level
					features, _ := parsed["features"].(map[string]any)
					mmHashes, _ := features["mm_hashes"].(map[string]any)
					imageHashes, _ := mmHashes[coordsteps.ModalityImage].([]any)
					hash, _ := imageHashes[0].(string)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"ec_transfer_params": map[string]any{
							hash: map[string]any{"peer_host": "10.0.0.1", "peer_port": 5501},
						},
					})
				case gateway.PhasePrefill:
					body, _ := io.ReadAll(r.Body)
					var parsed map[string]any
					_ = json.Unmarshal(body, &parsed)
					mu.Lock()
					capturedPrefillBody = parsed
					mu.Unlock()
					_ = json.NewEncoder(w).Encode(map[string]any{
						"kv_transfer_params": map[string]any{
							"block_id":  "abc123",
							"peer_host": "10.0.0.2",
							"peer_port": 5502,
						},
					})
				case gateway.PhaseDecode:
					_ = json.NewEncoder(w).Encode(map[string]any{
						"choices": []map[string]any{
							{"message": map[string]any{"role": "assistant", "content": "Hello!"}},
						},
					})
				default:
					http.Error(w, "not found", 404)
				}
			}))
			defer gatewayServer.Close()

			gwClient := gateway.New(config.GatewayConfig{Address: gatewayServer.URL, MaxIdleConnsPerHost: 10})

			stepConfigs := []config.StepConfig{
				{Type: "replace-media-urls", Params: map[string]any{"download_timeout": "5s"}},
				{Type: "render", Params: map[string]any{"endpoint": reqcommon.PathChatCompletions + "/render"}},
				{Type: "encode", Params: map[string]any{"use_openai_format": false, coordsteps.ParamECConnector: tc.ecConnector}},
				{Type: "prefill", Params: map[string]any{"use_openai_format": false, coordsteps.ParamKVConnector: tc.kvConnector, coordsteps.ParamECConnector: tc.ecConnector}},
				{Type: "decode", Params: map[string]any{"use_openai_format": false, coordsteps.ParamKVConnector: tc.kvConnector}},
			}

			pipelineSteps := make([]pipeline.Step, 0, len(stepConfigs))
			for _, sc := range stepConfigs {
				step, err := pipeline.Build(sc.Type, gwClient, sc.Params)
				if err != nil {
					t.Fatalf("building step %s: %v", sc.Type, err)
				}
				if ra, ok := step.(renderAware); ok {
					ra.SetServiceAddress(renderServer.URL)
				}
				pipelineSteps = append(pipelineSteps, step)
			}

			requestBody := `{
				"model": "test-model", "stream": false,
				"messages": [{"role": "user", "content": [
					{"type": "text", "text": "What is in this image?"},
					{"type": "image_url", "image_url": {"url": "data:image/png;base64,ZmFrZS1pbWFnZS1kYXRh"}}
				]}]
			}`

			recorder := httptest.NewRecorder()
			reqCtx := &pipeline.RequestContext{
				RequestID:        "test-" + tc.kvConnector + "+" + tc.ecConnector,
				OriginalPath:     reqcommon.PathChatCompletions,
				OriginalBody:     []byte(requestBody),
				Model:            "test-model",
				KVTransferParams: make(map[string]any),
				ResponseWriter:   recorder,
			}
			_ = json.Unmarshal([]byte(requestBody), &reqCtx.Body)

			if err := pipeline.New(pipelineSteps).Execute(t.Context(), reqCtx); err != nil {
				t.Fatalf("pipeline failed: %v", err)
			}

			respBody, _ := io.ReadAll(recorder.Result().Body)
			if !strings.Contains(string(respBody), "Hello!") {
				t.Fatalf("expected 'Hello!' in response, got: %s", respBody)
			}

			if tc.wantECInPrefill {
				if len(reqCtx.ECTransferParams) == 0 {
					t.Error("expected ECTransferParams to be populated")
				}
			} else {
				if len(reqCtx.ECTransferParams) != 0 {
					t.Errorf("expected ECTransferParams to be empty, got %d entries", len(reqCtx.ECTransferParams))
				}
			}
			if len(reqCtx.KVTransferParams) == 0 {
				t.Error("expected KVTransferParams to be populated")
			}

			mu.Lock()
			captured := capturedPrefillBody
			mu.Unlock()
			if captured == nil {
				t.Fatal("prefill was not called")
			}
			// Generate format nests transfer params in sampling_params.extra_args.
			var hasEC bool
			if sp, ok := captured["sampling_params"].(map[string]any); ok {
				if ea, ok := sp["extra_args"].(map[string]any); ok {
					_, hasEC = ea["ec_transfer_params"]
				}
			}
			if tc.wantECInPrefill && !hasEC {
				t.Error("expected ec_transfer_params in prefill body sampling_params.extra_args")
			}
			if !tc.wantECInPrefill && hasEC {
				t.Error("unexpected ec_transfer_params in prefill body sampling_params.extra_args")
			}
		})
	}
}

func TestFullPipeline_Integration(t *testing.T) {
	renderServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token_ids": []int{1, 32000, 32000, 32000, 2345, 6789},
			"features": map[string]any{
				"mm_hashes":       map[string][]string{coordsteps.ModalityImage: {"vllm-hash-img0"}},
				"mm_placeholders": map[string][]any{coordsteps.ModalityImage: {map[string]any{"offset": 1, "length": 3}}},
				"kwargs_data":     map[string][]string{coordsteps.ModalityImage: {"dGVuc29yLWRhdGE="}},
			},
		})
	}))
	defer renderServer.Close()

	gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		phase := r.Header.Get(gateway.EPPProfileHeader)
		switch phase {
		case gateway.PhaseEncode:
			body, _ := io.ReadAll(r.Body)
			var parsed map[string]any
			_ = json.Unmarshal(body, &parsed)
			features, _ := parsed["features"].(map[string]any)
			mmHashes, _ := features["mm_hashes"].(map[string]any)
			imageHashes, _ := mmHashes[coordsteps.ModalityImage].([]any)
			hash, _ := imageHashes[0].(string)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ec_transfer_params": map[string]any{
					hash: map[string]any{"peer_host": "10.0.0.1", "peer_port": 5501},
				},
			})
		case gateway.PhasePrefill:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"kv_transfer_params": map[string]any{
					"block_id":  "abc123",
					"peer_host": "10.0.0.2",
					"peer_port": 5502,
				},
			})
		case gateway.PhaseDecode:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"choices": []map[string]any{
					{"message": map[string]any{"role": "assistant", "content": "Hello!"}},
				},
			})
		default:
			http.Error(w, "not found", 404)
		}
	}))
	defer gatewayServer.Close()

	gwClient := gateway.New(config.GatewayConfig{
		Address:             gatewayServer.URL,
		MaxIdleConnsPerHost: 10,
	})

	stepConfigs := []config.StepConfig{
		{Type: "replace-media-urls", Params: map[string]any{"download_timeout": "5s"}},
		{Type: "render", Params: map[string]any{"endpoint": reqcommon.PathChatCompletions + "/render"}},
		{Type: "encode", Params: map[string]any{"use_openai_format": false, coordsteps.ParamECConnector: ec.NIXL}},
		{Type: "prefill", Params: map[string]any{"use_openai_format": false, coordsteps.ParamECConnector: ec.NIXL}},
		{Type: "decode", Params: map[string]any{"use_openai_format": false}},
	}

	pipelineSteps := make([]pipeline.Step, 0, len(stepConfigs))
	for _, sc := range stepConfigs {
		step, err := pipeline.Build(sc.Type, gwClient, sc.Params)
		if err != nil {
			t.Fatalf("building step %s: %v", sc.Type, err)
		}

		if ra, ok := step.(renderAware); ok {
			ra.SetServiceAddress(renderServer.URL)
		}

		pipelineSteps = append(pipelineSteps, step)
	}

	p := pipeline.New(pipelineSteps)

	requestBody := `{
		"model": "test-model",
		"stream": false,
		"messages": [
			{
				"role": "user",
				"content": [
					{"type": "text", "text": "What is in this image?"},
					{"type": "image_url", "image_url": {"url": "data:image/png;base64,ZmFrZS1pbWFnZS1kYXRh"}}
				]
			}
		]
	}`

	recorder := httptest.NewRecorder()
	reqCtx := &pipeline.RequestContext{
		RequestID:        "test-123",
		OriginalPath:     reqcommon.PathChatCompletions,
		OriginalBody:     []byte(requestBody),
		Stream:           false,
		Model:            "test-model",
		KVTransferParams: make(map[string]any),
		ResponseWriter:   recorder,
	}

	_ = json.Unmarshal([]byte(requestBody), &reqCtx.Body)

	err := p.Execute(t.Context(), reqCtx)
	if err != nil {
		t.Fatalf("pipeline execution failed: %v", err)
	}

	result := recorder.Result()
	if result.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", result.StatusCode)
	}

	respBody, _ := io.ReadAll(result.Body)
	if !strings.Contains(string(respBody), "Hello!") {
		t.Fatalf("expected response to contain 'Hello!', got: %s", string(respBody))
	}

	if len(reqCtx.ECTransferParams) == 0 {
		t.Fatal("expected ECTransferParams to be populated")
	}
	if len(reqCtx.KVTransferParams) == 0 {
		t.Fatal("expected KVTransferParams to be populated")
	}
}

// TestAttribution_NonStreaming tests end-to-end attribution with a non-streaming
// decode-only pipeline: hook populates reqCtx fields and emits metrics.
func TestAttribution_NonStreaming(t *testing.T) {
	coordmetrics.ResetAttribution()
	reg := prometheus.NewRegistry()
	coordmetrics.RegisterAttribution(reg, false)
	t.Cleanup(coordmetrics.ResetAttribution)
	coordmetrics.Reset()

	fakeGW := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "serving-model",
			"usage": map[string]any{
				"prompt_tokens":     int(20),
				"completion_tokens": int(10),
			},
			"choices": []map[string]any{
				{"message": map[string]any{"content": "hi"}},
			},
		})
	}))
	defer fakeGW.Close()

	hook := coordsteps.NewAttributionHook(true, "test-ns")
	gwClient := gateway.New(config.GatewayConfig{Address: fakeGW.URL, MaxIdleConnsPerHost: 2})
	step, err := pipeline.Build(coordsteps.DecodeStepName, gwClient, map[string]any{"use_openai_format": true})
	if err != nil {
		t.Fatalf("build decode step: %v", err)
	}
	step.(coordsteps.AttributionHookSetter).SetAttributionHook(hook)

	recorder := httptest.NewRecorder()
	reqCtx := &pipeline.RequestContext{
		RequestID:        "attribution-nonstreaming",
		OriginalPath:     reqcommon.PathChatCompletions,
		Body:             map[string]any{"model": "requested-model", "messages": []any{}},
		OriginalBody:     []byte(`{"model":"requested-model","messages":[]}`),
		Model:            "requested-model",
		Stream:           false,
		TenantID:         "tenant1",
		UserID:           "user1",
		WorkloadID:       "wl1",
		KVTransferParams: make(map[string]any),
		ResponseWriter:   recorder,
	}

	if err := pipeline.New([]pipeline.Step{step}).Execute(t.Context(), reqCtx); err != nil {
		t.Fatalf("pipeline execute: %v", err)
	}

	// Attribution hook must have populated reqCtx.
	if reqCtx.CompletionTokens != 10 {
		t.Errorf("CompletionTokens = %d, want 10", reqCtx.CompletionTokens)
	}
	if reqCtx.PromptTokensFromBody != 20 {
		t.Errorf("PromptTokensFromBody = %d, want 20", reqCtx.PromptTokensFromBody)
	}

	// Attributed metrics must be present on the fresh registry.
	mf, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	names := make(map[string]bool)
	for _, m := range mf {
		names[m.GetName()] = true
	}
	if !names["llm_d_coordinator_request_input_tokens_attributed_total"] {
		t.Error("expected input attributed metric")
	}
	if !names["llm_d_coordinator_request_output_tokens_attributed_total"] {
		t.Error("expected output attributed metric")
	}
}

// TestAttribution_Disabled tests that when the hook is nil, the decode step
// behaves identically to pre-attribution state.
func TestAttribution_Disabled(t *testing.T) {
	coordmetrics.ResetAttribution()
	t.Cleanup(coordmetrics.ResetAttribution)

	fakeGW := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": "serving-model",
			"choices": []map[string]any{
				{"message": map[string]any{"content": "hi"}},
			},
		})
	}))
	defer fakeGW.Close()

	gwClient := gateway.New(config.GatewayConfig{Address: fakeGW.URL, MaxIdleConnsPerHost: 2})
	step, err := pipeline.Build(coordsteps.DecodeStepName, gwClient, map[string]any{"use_openai_format": true})
	if err != nil {
		t.Fatalf("build decode step: %v", err)
	}
	// No hook set — attribution disabled.

	recorder := httptest.NewRecorder()
	reqCtx := &pipeline.RequestContext{
		RequestID:        "attribution-disabled",
		OriginalPath:     reqcommon.PathChatCompletions,
		Body:             map[string]any{"model": "m", "messages": []any{}},
		OriginalBody:     []byte(`{"model":"m","messages":[]}`),
		Model:            "m",
		KVTransferParams: make(map[string]any),
		ResponseWriter:   recorder,
	}

	if err := pipeline.New([]pipeline.Step{step}).Execute(t.Context(), reqCtx); err != nil {
		t.Fatalf("pipeline execute: %v", err)
	}
	// No panic; token counts stay at zero-value (not set by hook).
	if reqCtx.CompletionTokens != 0 {
		t.Errorf("CompletionTokens = %d, want 0 (hook was nil)", reqCtx.CompletionTokens)
	}
}

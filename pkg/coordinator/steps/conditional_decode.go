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
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"

	"github.com/llm-d/llm-d-router/pkg/coordinator/gateway"
	coordmetrics "github.com/llm-d/llm-d-router/pkg/coordinator/metrics"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

const ConditionalDecodeStepName = "conditional-decode"

func init() {
	pipeline.Register(ConditionalDecodeStepName, NewConditionalDecodeStep)
}

type ConditionalDecodeStep struct {
	useOpenAIFormat bool
	gwClient        *gateway.Client
	// Attribution hook: captures completion_tokens from the response body for cost
	// attribution metrics. Injected by the builder when request_attribution.enabled
	// is true; nil otherwise. See attribution.go.
	attribution *AttributionHook
}

// SetAttributionHook implements AttributionHookSetter. Must be called once
// during construction (by the builder), before the step handles any request.
// Calling it after the step begins serving requests is a data race.
func (s *ConditionalDecodeStep) SetAttributionHook(h *AttributionHook) {
	s.attribution = h
}

func NewConditionalDecodeStep(gwClient *gateway.Client, params map[string]any) (pipeline.Step, error) {
	if gwClient == nil {
		return nil, errors.New("conditional-decode: gateway client is required")
	}
	useOpenAI, err := parseUseOpenAIFormat(params)
	if err != nil {
		return nil, err
	}
	return &ConditionalDecodeStep{useOpenAIFormat: useOpenAI, gwClient: gwClient}, nil
}

func (s *ConditionalDecodeStep) Name() string { return ConditionalDecodeStepName }

func (s *ConditionalDecodeStep) Execute(ctx context.Context, reqCtx *pipeline.RequestContext) error {
	logger := log.FromContext(ctx).WithName(ConditionalDecodeStepName)

	body := maps.Clone(reqCtx.Body)
	if err := s.prepareBody(reqCtx, body, resolveFormat(s.useOpenAIFormat, reqCtx.OriginalPath)); err != nil {
		return err
	}

	logger.V(logutil.DEFAULT).Info("sending request", "path", reqCtx.OriginalPath)

	proxyReq, err := newDecodeProxyRequest(ctx, logger, ConditionalDecodeStepName, reqCtx, s.gwClient, body, map[string]string{"Prefer": "if-available"})
	if err != nil {
		return err
	}

	var cacheMiss bool
	// probe is the existing cache-miss detection closure. Attribution must compose
	// with it, never replace it: replacing it removes cache-miss detection entirely,
	// so the 412 would be streamed to the client as the final response.
	probe := func(resp *http.Response) error {
		switch {
		case resp.StatusCode == http.StatusPreconditionFailed:
			cacheMiss = true
			coordmetrics.IncConditionalDecodeProbes(coordmetrics.ProbeResultDeferred)
			return errCacheMiss
		case resp.StatusCode >= http.StatusBadRequest:
			// Worker error (any 4xx/5xx except 412): the response is still
			// streamed to the client, but the outcome is not a served hit.
			coordmetrics.IncConditionalDecodeProbes(coordmetrics.ProbeResultError)
		default:
			coordmetrics.IncConditionalDecodeProbes(coordmetrics.ProbeResultServed)
		}
		return nil
	}
	// Attribution hook: captures completion_tokens from the response body for cost
	// attribution metrics. Injected by the builder when request_attribution.enabled
	// is true; nil otherwise. See attribution.go.
	// Compose with the probe: ModifyResponseFor installs the tee first, then
	// calls probe. The ordering is load-bearing — tee before delegation so the
	// probe's errCacheMiss does not skip setup. probe's error is returned verbatim.
	transport := instrumentedTransport(s.gwClient.Transport(), coordmetrics.UpstreamConditionalDecode)
	modifyResp := func(resp *http.Response) error { return probe(resp) }
	if s.attribution != nil {
		modifyResp = s.attribution.ModifyResponseFor(reqCtx, probe)
	}
	proxy, out := newDecodeProxy(logger, transport, modifyResp)
	proxy.ServeHTTP(reqCtx.ResponseWriter, proxyReq)

	if cacheMiss {
		logger.V(logutil.DEFAULT).Info("cache miss (412), continuing pipeline")
		return nil // pipeline continues to decode; that step's hook emits attribution
	}
	if out.TransportErr != nil {
		coordmetrics.IncConditionalDecodeProbes(coordmetrics.ProbeResultTransportError)
		return &pipeline.UpstreamStreamedError{Step: ConditionalDecodeStepName, Cause: out.TransportErr}
	}
	if out.Status >= http.StatusBadRequest {
		return &pipeline.UpstreamStreamedError{Step: ConditionalDecodeStepName, StatusCode: out.Status}
	}
	if s.attribution != nil {
		s.attribution.Emit(reqCtx)
	}

	logger.V(logutil.DEFAULT).Info("cache hit, response forwarded")
	return pipeline.ErrPipelineDone
}

func (s *ConditionalDecodeStep) prepareBody(reqCtx *pipeline.RequestContext, body map[string]any, format reqcommon.APIType) error {
	switch format {
	case reqcommon.APITypeChatCompletions:
		if len(reqCtx.TokenIDs) > 0 {
			tokens := map[string]any{
				"token_ids": reqCtx.TokenIDs,
			}
			if features := buildMMFeatures(reqCtx.MultimodalEntries, false); features != nil {
				tokens["features"] = features
			}
			body["tokens"] = tokens
		}
	case reqcommon.APITypeCompletions:
		if len(reqCtx.TokenIDs) > 0 {
			body["prompt"] = reqCtx.TokenIDs
		}
	case reqcommon.APITypeGenerate:
		// The client's generate body already carries token_ids.
	default:
		// resolveFormat only ever yields the three formats above.
		return fmt.Errorf("conditional-decode: unsupported request format %v", format)
	}
	return nil
}

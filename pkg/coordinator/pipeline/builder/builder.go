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

// Package builder assembles a coordinator pipeline from configuration. It lives
// apart from pipeline and steps because it depends on both: steps imports
// pipeline, so this glue cannot live in pipeline without an import cycle.
package builder

import (
	"fmt"

	"github.com/llm-d/llm-d-router/pkg/coordinator/config"
	"github.com/llm-d/llm-d-router/pkg/coordinator/gateway"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
	"github.com/llm-d/llm-d-router/pkg/coordinator/steps"
	"github.com/llm-d/llm-d-router/pkg/coordinator/steps/asyncbroker"
)

// validatePipeline rejects configurations that cannot work before any step runs.
// The async-broker step must intercept requests before any processing step
// touches them (queued bodies are stored verbatim, and passthrough stamping
// must precede routing), so it is only valid in first position. The tokens-in
// format (use_openai_format=false) sends token IDs that only the render step
// produces, so it requires a render step in the pipeline.
// When attributionEnabled is true, the pipeline must contain at least one decode
// step (decode or conditional-decode); attribution is captured from the decode
// response body and cannot function without one.
func validatePipeline(p config.PipelineConfig, attributionEnabled bool) error {
	for i, s := range p.Steps {
		if s.Type == asyncbroker.StepName && i > 0 {
			return fmt.Errorf("the %q step must be the first pipeline step, found it at position %d", asyncbroker.StepName, i+1)
		}
	}
	if !p.UseOpenAIFormat {
		hasRender := false
		for _, s := range p.Steps {
			if s.Type == steps.RenderStepName {
				hasRender = true
				break
			}
		}
		if !hasRender {
			return fmt.Errorf("pipeline.use_openai_format=false requires a %q step (the tokens-in format sends token IDs that render produces)", steps.RenderStepName)
		}
	}
	if attributionEnabled {
		// Attribution is captured from the decode response body, so a pipeline with
		// neither decode step can never emit an attribution record. Enabling
		// attribution against such a pipeline is a config error, not a silent no-op.
		hasDecodeStep := false
		for _, s := range p.Steps {
			if s.Type == steps.DecodeStepName || s.Type == steps.ConditionalDecodeStepName {
				hasDecodeStep = true
				break
			}
		}
		if !hasDecodeStep {
			return fmt.Errorf("request_attribution.enabled=true requires a %q or %q step in the pipeline (attribution is captured from the decode response body)", steps.DecodeStepName, steps.ConditionalDecodeStepName)
		}
	}
	return nil
}

func mergeConnectorDefaults(params map[string]any, kvConnector, ecConnector string) map[string]any {
	out := make(map[string]any, len(params))
	for k, v := range params {
		out[k] = v
	}
	if _, ok := out[steps.ParamKVConnector]; !ok && kvConnector != "" {
		out[steps.ParamKVConnector] = kvConnector
	}
	if _, ok := out[steps.ParamECConnector]; !ok && ecConnector != "" {
		out[steps.ParamECConnector] = ecConnector
	}
	return out
}

// Build validates cfg.Pipeline and constructs its steps in order.
// hook is the attribution hook constructed by main.go; it is nil when
// request_attribution.enabled is false and Build then behaves exactly as
// it does without attribution.
//
// Attribution hooks are injected here, after step construction, rather than
// requiring operators to configure them in their pipeline YAML. The hook must
// be on the decode step(s) specifically — it is not a generic pipeline concern.
// When attribution is disabled, hooks are nil and both steps behave identically
// to their pre-attribution state. See RequestAttributionConfig.
func Build(cfg *config.Config, gwClient *gateway.Client, hook *steps.AttributionHook) ([]pipeline.Step, error) {
	if err := validatePipeline(cfg.Pipeline, cfg.RequestAttribution.Enabled); err != nil {
		return nil, err
	}

	var pipelineSteps []pipeline.Step
	for _, stepCfg := range cfg.Pipeline.Steps {
		params := mergeConnectorDefaults(stepCfg.Params, cfg.Pipeline.KVConnector, cfg.Pipeline.ECConnector)
		if _, ok := params["use_openai_format"]; !ok {
			params["use_openai_format"] = cfg.Pipeline.UseOpenAIFormat
		}
		step, err := pipeline.Build(stepCfg.Type, gwClient, params)
		if err != nil {
			return nil, err
		}

		pipelineSteps = append(pipelineSteps, step)
	}

	// Inject the attribution hook into decode steps after construction.
	// The StepFactory interface accepts only (gwClient, params), so the hook
	// cannot be passed via the factory. Instead we use the AttributionHookSetter
	// interface, which both DecodeStep and ConditionalDecodeStep implement.
	if hook != nil {
		for _, step := range pipelineSteps {
			if setter, ok := step.(steps.AttributionHookSetter); ok {
				setter.SetAttributionHook(hook)
			}
		}
	}

	return pipelineSteps, nil
}

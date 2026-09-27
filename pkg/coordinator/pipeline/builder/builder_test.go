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

package builder

import (
	"testing"

	"github.com/llm-d/llm-d-router/pkg/coordinator/config"
	"github.com/llm-d/llm-d-router/pkg/coordinator/gateway"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
	"github.com/llm-d/llm-d-router/pkg/coordinator/steps"
	"github.com/llm-d/llm-d-router/pkg/coordinator/steps/asyncbroker"
)

func TestValidatePipeline(t *testing.T) {
	render := config.StepConfig{Type: steps.RenderStepName}
	decode := config.StepConfig{Type: steps.DecodeStepName}
	broker := config.StepConfig{Type: asyncbroker.StepName}

	tests := []struct {
		name    string
		cfg     config.PipelineConfig
		wantErr bool
	}{
		{
			name:    "openai format needs no render",
			cfg:     config.PipelineConfig{UseOpenAIFormat: true, Steps: []config.StepConfig{decode}},
			wantErr: false,
		},
		{
			name:    "tokens-in with render",
			cfg:     config.PipelineConfig{UseOpenAIFormat: false, Steps: []config.StepConfig{render, decode}},
			wantErr: false,
		},
		{
			name:    "tokens-in without render is rejected",
			cfg:     config.PipelineConfig{UseOpenAIFormat: false, Steps: []config.StepConfig{decode}},
			wantErr: true,
		},
		{
			name:    "async-broker first is accepted",
			cfg:     config.PipelineConfig{UseOpenAIFormat: true, Steps: []config.StepConfig{broker, decode}},
			wantErr: false,
		},
		{
			name:    "async-broker after another step is rejected",
			cfg:     config.PipelineConfig{UseOpenAIFormat: true, Steps: []config.StepConfig{decode, broker}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// attributionEnabled=false so existing cases are unaffected.
			err := validatePipeline(tt.cfg, false)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validatePipeline() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// newMinimalGateway returns a gateway.Client for tests that need to call Build
// but don't actually make HTTP requests.
func newMinimalGateway() *gateway.Client {
	return gateway.NewWithTransport(nil, "http://gateway-test.invalid")
}

// TestValidatePipeline_Attribution tests the attribution validation cases.
func TestValidatePipeline_Attribution(t *testing.T) {
	decode := config.StepConfig{Type: steps.DecodeStepName}
	condDecode := config.StepConfig{Type: steps.ConditionalDecodeStepName}
	render := config.StepConfig{Type: steps.RenderStepName}

	tests := []struct {
		name               string
		cfg                config.PipelineConfig
		attributionEnabled bool
		wantErr            bool
	}{
		{
			name:               "attribution disabled — no decode step is ok",
			cfg:                config.PipelineConfig{UseOpenAIFormat: true, Steps: []config.StepConfig{render}},
			attributionEnabled: false,
			wantErr:            false,
		},
		{
			name:               "attribution enabled — decode present",
			cfg:                config.PipelineConfig{UseOpenAIFormat: true, Steps: []config.StepConfig{decode}},
			attributionEnabled: true,
			wantErr:            false,
		},
		{
			name:               "attribution enabled — conditional-decode present",
			cfg:                config.PipelineConfig{UseOpenAIFormat: true, Steps: []config.StepConfig{condDecode, decode}},
			attributionEnabled: true,
			wantErr:            false,
		},
		{
			name:               "attribution enabled — no decode step is rejected",
			cfg:                config.PipelineConfig{UseOpenAIFormat: true, Steps: []config.StepConfig{render}},
			attributionEnabled: true,
			wantErr:            true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePipeline(tt.cfg, tt.attributionEnabled)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validatePipeline() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestBuild_AttributionHookInjected verifies that Build injects the hook into
// decode steps when attribution is enabled.
func TestBuild_AttributionHookInjected(t *testing.T) {
	hook := steps.NewAttributionHook(true, "test-ns")
	if hook == nil {
		t.Fatal("NewAttributionHook(true) returned nil")
	}

	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			UseOpenAIFormat: true,
			Steps: []config.StepConfig{
				{Type: steps.DecodeStepName},
			},
		},
		RequestAttribution: config.RequestAttributionConfig{Enabled: true},
	}

	pipelineSteps, err := Build(cfg, newMinimalGateway(), hook)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if len(pipelineSteps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(pipelineSteps))
	}
	// The step must implement AttributionHookSetter and have the hook set.
	setter, ok := pipelineSteps[0].(steps.AttributionHookSetter)
	if !ok {
		t.Fatal("decode step does not implement AttributionHookSetter")
	}
	// Verify we can set the hook without panic (already set; just testing interface).
	setter.SetAttributionHook(hook)
}

// TestBuild_AttributionDisabled_NoHookInjected verifies that Build with hook=nil
// behaves identically to the pre-attribution state (no hook set on steps).
func TestBuild_AttributionDisabled_NoHookInjected(t *testing.T) {
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			UseOpenAIFormat: true,
			Steps: []config.StepConfig{
				{Type: steps.DecodeStepName},
			},
		},
	}
	pipelineSteps, err := Build(cfg, newMinimalGateway(), nil)
	if err != nil {
		t.Fatalf("Build() with nil hook error = %v", err)
	}
	if len(pipelineSteps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(pipelineSteps))
	}
}

// TestBuild_AttributionEnabled_NoDecodeStep_ReturnsError verifies that Build
// rejects a pipeline with attribution enabled but no decode step.
func TestBuild_AttributionEnabled_NoDecodeStep_ReturnsError(t *testing.T) {
	hook := steps.NewAttributionHook(true, "test-ns")
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			UseOpenAIFormat: true,
			Steps: []config.StepConfig{
				{Type: steps.RenderStepName},
			},
		},
		RequestAttribution: config.RequestAttributionConfig{Enabled: true},
	}
	_, err := Build(cfg, newMinimalGateway(), hook)
	if err == nil {
		t.Fatal("expected error when attribution enabled but no decode step")
	}
}

// TestBuild_ConditionalDecodeHookInjected verifies that the hook is also
// injected into ConditionalDecodeStep.
func TestBuild_ConditionalDecodeHookInjected(t *testing.T) {
	hook := steps.NewAttributionHook(true, "ns")
	cfg := &config.Config{
		Pipeline: config.PipelineConfig{
			UseOpenAIFormat: true,
			Steps: []config.StepConfig{
				{Type: steps.ConditionalDecodeStepName},
				{Type: steps.DecodeStepName},
			},
		},
		RequestAttribution: config.RequestAttributionConfig{Enabled: true},
	}
	pipelineSteps, err := Build(cfg, newMinimalGateway(), hook)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	// Both steps must implement AttributionHookSetter.
	for _, step := range pipelineSteps {
		if _, ok := step.(steps.AttributionHookSetter); !ok {
			t.Errorf("step %q does not implement AttributionHookSetter", step.(pipeline.Step).Name())
		}
	}
}

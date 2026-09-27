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

// attribution.go implements request attribution for the coordinator's decode steps.
//
// Architecture (Option C): attribution is implemented as a ModifyResponse hook
// injected into DecodeStep and ConditionalDecodeStep at construction time, not as a
// wrapper step in the pipeline slice. See Decision 1 in the implementation plan for
// the full rationale. Key properties:
//
//   - Neither decode step changes its Name(), so execution_path_total and
//     step_running/step_duration_seconds metrics are unaffected.
//   - ConditionalDecodeStep's cache-miss (412) path is handled correctly: the hook
//     fires only when ModifyResponse is called (i.e. a real response was received),
//     not on the cache-miss fallthrough.
//   - ErrPipelineDone (cache hit) is handled correctly: ModifyResponse fires before
//     ServeHTTP returns, which is before ErrPipelineDone is returned by Execute.
//
// Pipeline placement (Decision 2): the builder injects the hook into each decode step
// constructor when request_attribution.enabled is true. Operators need only set that
// config flag; no pipeline YAML changes are required.

package steps

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	coordmetrics "github.com/llm-d/llm-d-router/pkg/coordinator/metrics"
	"github.com/llm-d/llm-d-router/pkg/coordinator/pipeline"
)

const (
	// attributionSideBufferCap is the maximum number of bytes captured from a
	// non-streaming response body. A usage object plus surrounding JSON is
	// at most a few KB; 256 KB is a generous ceiling that eliminates practical
	// risk of OOM while being far more than any response metadata needs.
	// When the cap is reached the capture is marked truncated, and Emit uses
	// fallback token values. The forwarded stream is unaffected: the tee's
	// write target going quiet does not alter what the proxy reads or writes.
	attributionSideBufferCap = 256 * 1024
)

// AttributionHook is an optional post-response callback injected into DecodeStep
// and ConditionalDecodeStep. It is called with the response after the proxy has
// finished writing to the client.
type AttributionHook struct {
	namespace   string
	logger      logr.Logger // named "coordinator.attribution"
	parseStates sync.Map    // keyed by *pipeline.RequestContext; value *parseState
}

// AttributionHookSetter is implemented by pipeline steps that accept an
// optional AttributionHook. The builder calls SetAttributionHook after
// step construction when request_attribution.enabled is true.
type AttributionHookSetter interface {
	SetAttributionHook(h *AttributionHook)
}

// NewAttributionHook returns a new AttributionHook, or nil when enabled is false.
// Taking the bare flag rather than the config struct keeps the steps package
// free of a config import.
func NewAttributionHook(enabled bool, namespace string) *AttributionHook {
	if !enabled {
		return nil
	}
	return &AttributionHook{
		namespace: namespace,
		logger:    ctrl.Log.WithName("coordinator.attribution"),
	}
}

// parseState holds the side buffer or stream-tracker set up inside
// ModifyResponseFor and read by Emit after proxy.ServeHTTP returns.
type parseState struct {
	// non-streaming: bounded side buffer
	buf *cappedBuffer
	// streaming: retains only the last non-empty data: line
	tracker   *lastChunkTracker
	streaming bool
}

// cappedBuffer is a write target that accumulates up to limit bytes and then
// silently discards further writes, marking itself truncated. It is used as the
// io.TeeReader destination for non-streaming responses.
type cappedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if c.truncated {
		return len(p), nil
	}
	space := c.limit - c.buf.Len()
	if space <= 0 {
		c.truncated = true
		return len(p), nil
	}
	if len(p) > space {
		_, _ = c.buf.Write(p[:space])
		c.truncated = true
		return len(p), nil
	}
	return c.buf.Write(p)
}

// lastChunkTracker is a write target used for streaming (SSE) responses. It
// retains only the last non-empty "data: ..." line it has seen. Storing a
// single line keeps the streaming attribution path to a fixed, small memory
// footprint regardless of total response size.
//
// SSE frames may be split across multiple Write calls (e.g. when the HTTP proxy
// flushes mid-line). The pending field buffers an incomplete line between calls
// so no data: line is ever missed due to a TCP split.
type lastChunkTracker struct {
	last    []byte
	pending []byte // incomplete line carried over from previous Write
}

// dataPrefix is the SSE payload prefix this tracker scans for.
var dataPrefix = []byte("data: ")

func (t *lastChunkTracker) Write(p []byte) (int, error) {
	n := len(p) // capture original length to return to the io.Writer caller
	// Prepend any leftover bytes from the previous Write so line boundaries
	// are always resolved against a complete buffer.
	if len(t.pending) > 0 {
		p = append(t.pending, p...) //nolint:gocritic // intentional re-slice; append is safe here
		t.pending = nil
	}

	for len(p) > 0 {
		idx := bytes.IndexByte(p, '\n')
		if idx < 0 {
			// No newline yet — save remainder for the next Write.
			t.pending = append(t.pending[:0], p...)
			break
		}
		line := p[:idx]
		p = p[idx+1:]

		// Strip optional trailing CR (CRLF line endings).
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}

		if bytes.HasPrefix(line, dataPrefix) {
			payload := line[len(dataPrefix):]
			if !bytes.Equal(payload, []byte("[DONE]")) {
				// Copy so t.last is not aliased to a caller-supplied buffer.
				t.last = append(t.last[:0], payload...)
			}
		}
	}
	return n, nil
}

// ModifyResponseFor returns a func(*http.Response) error suitable for passing
// to newDecodeProxy. It installs a TeeReader on resp.Body so the body bytes
// flow to the client unchanged while a copy accumulates in the side buffer (or
// the lastChunkTracker for streaming). Parsing happens later in Emit, after
// proxy.ServeHTTP has returned.
//
// next is the step's own modifyResponse (e.g. the ConditionalDecodeStep's
// cache-miss probe closure), or nil. The tee is installed before next is called,
// so the probe can return errCacheMiss without skipping the setup. next's error
// is returned verbatim — swallowing errCacheMiss would break the miss fallthrough.
func (h *AttributionHook) ModifyResponseFor(reqCtx *pipeline.RequestContext, next func(*http.Response) error) func(*http.Response) error {
	ps := &parseState{streaming: reqCtx.Stream}
	if reqCtx.Stream {
		ps.tracker = &lastChunkTracker{}
	} else {
		ps.buf = &cappedBuffer{limit: attributionSideBufferCap}
	}
	// Register the parseState before the closure is ever called so Emit can
	// always find it — even if ModifyResponse is called but the proxy returns
	// before Emit runs.
	h.parseStates.Store(reqCtx, ps)

	return func(resp *http.Response) error {
		// Install the tee before delegating to next so that the probe closure
		// (conditional-decode) can return errCacheMiss without leaving the tee
		// uninstalled. Even on a 412 the tee is harmless: nothing flows through
		// it because the proxy's ErrorHandler never copies a 412 body to the client.
		var teeTarget io.Writer
		if ps.streaming {
			teeTarget = ps.tracker
		} else {
			teeTarget = ps.buf
		}
		resp.Body = io.NopCloser(io.TeeReader(resp.Body, teeTarget))

		if next != nil {
			return next(resp)
		}
		return nil
	}
}

// Emit is called by the decode step immediately after proxy.ServeHTTP returns,
// when no transport error occurred and no upstream error status was received.
// It parses the captured response body (or the last SSE chunk for streaming),
// populates reqCtx with the derived token counts, then emits the Prometheus
// metrics and the structured log record.
//
// Called synchronously on the request goroutine — the client has already
// received its complete response, so client-perceived latency is zero.
// Do not spawn a goroutine (see Appendix B of the proposal).
func (h *AttributionHook) Emit(reqCtx *pipeline.RequestContext) {
	ps := h.loadAndDeleteParsed(reqCtx)

	var truncated bool
	if ps != nil {
		if ps.streaming {
			if ps.tracker != nil && len(ps.tracker.last) > 0 {
				h.parseChunk(ps.tracker.last, reqCtx)
			}
		} else {
			if ps.buf != nil {
				truncated = ps.buf.truncated
				if ps.buf.buf.Len() > 0 {
					h.parseBody(ps.buf.buf.Bytes(), reqCtx)
				}
			}
		}
	}

	// Determine prompt token count: prefer render-derived TokenIDs, fall back
	// to prompt_tokens parsed from the decode response body.
	promptTokens := len(reqCtx.TokenIDs)
	if promptTokens == 0 {
		promptTokens = reqCtx.PromptTokensFromBody
	}

	coordmetrics.RecordAttributedInputTokens(
		reqCtx.Model, reqCtx.TenantID, reqCtx.WorkloadID, h.namespace, reqCtx.UserID, promptTokens,
	)
	coordmetrics.RecordAttributedOutputTokens(
		reqCtx.Model, reqCtx.TenantID, reqCtx.WorkloadID, h.namespace, reqCtx.UserID, reqCtx.CompletionTokens,
	)

	logFields := []any{
		"tenant_id", reqCtx.TenantID,
		"user_id", reqCtx.UserID,
		"workload_id", reqCtx.WorkloadID,
		"model_name", reqCtx.Model,
		"namespace", h.namespace,
		"prompt_tokens", promptTokens,
		"completion_tokens", reqCtx.CompletionTokens,
		"request_id", reqCtx.RequestID,
	}
	if truncated {
		logFields = append(logFields, "body_truncated", true)
	}
	h.logger.Info("request.complete", logFields...)
}

// parseBody extracts usage.prompt_tokens and usage.completion_tokens from a
// non-streaming JSON response body and stores them on reqCtx.
func (h *AttributionHook) parseBody(data []byte, reqCtx *pipeline.RequestContext) {
	var resp struct {
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return
	}
	reqCtx.PromptTokensFromBody = resp.Usage.PromptTokens
	reqCtx.CompletionTokens = resp.Usage.CompletionTokens
}

// parseChunk extracts usage.completion_tokens (and optionally prompt_tokens)
// from the last SSE data chunk (which for vLLM carries the usage summary when
// include_usage is set).
func (h *AttributionHook) parseChunk(data []byte, reqCtx *pipeline.RequestContext) {
	var chunk struct {
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(data, &chunk); err != nil {
		return
	}
	reqCtx.CompletionTokens = chunk.Usage.CompletionTokens
	if chunk.Usage.PromptTokens > 0 {
		reqCtx.PromptTokensFromBody = chunk.Usage.PromptTokens
	}
}

func (h *AttributionHook) loadAndDeleteParsed(reqCtx *pipeline.RequestContext) *parseState {
	v, loaded := h.parseStates.LoadAndDelete(reqCtx)
	if !loaded {
		return nil
	}
	if ps, ok := v.(*parseState); ok {
		return ps
	}
	return nil
}

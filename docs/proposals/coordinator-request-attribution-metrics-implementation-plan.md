# Request Attribution Metrics — Implementation Plan

**Authors**: Sima Nadler (_IBM_)

**Related proposal**: [`coordinator-request-attribution-metrics.md`](coordinator-request-attribution-metrics.md)

**Related issue**: [llm-d/llm-d-router#2741](https://github.com/llm-d/llm-d-router/issues/2741)

---

## Summary

This document is the implementation plan for
[`coordinator-request-attribution-metrics.md`](coordinator-request-attribution-metrics.md). It
translates the design into a sequenced, file-by-file set of changes with all
implementation decisions resolved.

## Resolved Decisions

Two design choices in the proposal were left open for the implementer. Both are
resolved here.

### Decision 1: Attribution architecture — ModifyResponse hook (Option C)

Attribution data is captured via a **`ModifyResponse` hook injected into `DecodeStep` and
`ConditionalDecodeStep` at construction time**, not via a wrapper step that sits in the
pipeline slice. Both steps pass a `modifyResponse func(*http.Response) error` to
`newDecodeProxy`. When attribution is enabled, that function:

1. Wraps `resp.Body` with `io.TeeReader` so the body can be read for parsing while still
   flowing through the proxy to the client.
2. Delegates to the step's own `modifyResponse` logic, if any, and returns its result
   unchanged.

Parsing happens later, in `Emit`, after `proxy.ServeHTTP` has returned and the body has
been copied through the tee. `Emit` stores `CompletionTokens` and `PromptTokensFromBody`
on `reqCtx`, then emits the Prometheus metrics and the structured log record.

> **`newDecodeProxy` takes exactly one `modifyResponse`.** `DecodeStep` passes `nil`
> today ([`decode.go:82`](../../pkg/coordinator/steps/decode.go)), but
> `ConditionalDecodeStep` passes a live closure that detects the 412 cache probe, sets
> its `cacheMiss` flag, returns `errCacheMiss`, and increments
> `conditional_decode_probes_total` for all four probe outcomes
> ([`conditional_decode.go:77-91`](../../pkg/coordinator/steps/conditional_decode.go)).
> Attribution must **compose with** that closure, never replace it: replacing it removes
> cache-miss detection entirely, so the 412 is streamed to the client as the final
> response and disaggregated prefill/decode never runs. Phase 6 specifies the composition
> and its required ordering.

**Rationale**: a wrapper step placed in the pipeline slice conflicts with two structural
properties of the existing pipeline:

- `pipeline.Execute` tracks which steps ran by `step.Name()` in `started[]` and
  `executed[]` maps. `classifyExecutionPath` checks for the literal strings `"decode"` and
  `"conditional-decode"`. A wrapper whose `Name()` returns `"attribution"` silently breaks
  `execution_path_total` and the per-step `step_running`/`step_duration_seconds` metrics
  for every request when attribution is enabled.

- `ConditionalDecodeStep.Execute` returns `nil` on a cache miss (412) so the pipeline
  continues to `decode`. A wrapper around only `conditional-decode` fires attribution
  before the real response exists. A wrapper around only `decode` misses cache-hit
  responses entirely. A dual-wrapper solution requires state coordination on `RequestContext`
  and still has the `Name()` problem.

Neither decode step changes its `Name()` under the `ModifyResponse` hook, so all existing
pipeline observability keeps working. The hook is installed on whichever step actually
serves the request, so the cache-hit and cache-miss paths need no coordination state. This
decision must be captured as a comment at the attribution hook injection site in
`pkg/coordinator/steps/decode.go` and `pkg/coordinator/steps/conditional_decode.go`.

### Decision 2: Pipeline placement — constructor injection by the builder

Operators enable attribution by setting `request_attribution.enabled: true` in config. The
builder injects the attribution hook into the `DecodeStep` and `ConditionalDecodeStep`
constructors — operators do not need to add anything to their `pipeline.steps` YAML.

**Rationale**: the attribution hook's placement inside the decode steps is an
implementation detail operators should not need to know. Requiring explicit YAML changes
would force every existing pipeline config to be updated to opt in.

The hook cannot travel through the existing `params` map the way `use_openai_format` does
([`builder.go:78-79`](../../pkg/coordinator/pipeline/builder/builder.go)), because it is a
typed dependency rather than a config scalar. Phase 7 therefore adds a post-construction
setter interface, which is a new pattern in this package rather than an existing one.

This decision must be documented as a comment at the injection site in
`pkg/coordinator/pipeline/builder/builder.go`.

---

## Implementation Phases

### Phase 1 — Configuration

**File**: `pkg/coordinator/config/config.go`

Add a new `RequestAttributionConfig` struct and a `RequestAttribution
RequestAttributionConfig` field to the top-level `Config` struct.

```go
type RequestAttributionConfig struct {
    // Enabled is the master gate for all attribution logic. When false, no header
    // extraction, no metric emission, no log record, and no stream_options injection
    // are performed. Default: false (opt-in).
    Enabled bool `mapstructure:"enabled"`

    // PrometheusUserIDLabel controls whether user_id is included as a label on the
    // attributed Prometheus counters. Set false (the default) for deployments with
    // unbounded end-user populations to avoid time-series cardinality explosion. Per-user
    // attribution remains fully available via the structured log regardless of this
    // setting. Default: false.
    PrometheusUserIDLabel bool `mapstructure:"prometheus_user_id_label"`
}
```

The struct is reached under the `request_attribution` key, snake_case like every other
top-level config key (`log_level`, `server.max_request_body_size`, `pipeline.use_openai_format`):

```go
type Config struct {
    LogLevel           int                      `mapstructure:"log_level"`
    Server             ServerConfig             `mapstructure:"server"`
    Gateway            GatewayConfig            `mapstructure:"gateway"`
    Pipeline           PipelineConfig           `mapstructure:"pipeline"`
    RequestAttribution RequestAttributionConfig `mapstructure:"request_attribution"`
}
```

YAML config keys:

```yaml
request_attribution:
  enabled: false                  # default: false (opt-in)
  prometheus_user_id_label: false # default: false (safe by default)
```

Both keys **must** be given explicit defaults in `Load`, alongside the existing
`v.SetDefault` calls:

```go
v.SetDefault("request_attribution.enabled", false)
v.SetDefault("request_attribution.prometheus_user_id_label", false)
```

These two lines are load-bearing for two separate reasons, and omitting them fails
silently in both:

- **The documented `false` default is otherwise unreachable.** Go's zero value for
  `PrometheusUserIDLabel` is `false`, so this is only an issue for the `Enabled` key, but
  symmetry and documentation clarity demand both are explicit. An operator who sets `enabled: true` and omits
  the label key would get `user_id` labels disabled — the opposite of the documented
  default.
- **`AutomaticEnv` does not bind keys viper has never seen.** Verified against
  `viper v1.21.0` (the version in `go.mod`) with this package's exact
  `SetEnvPrefix`/`SetEnvKeyReplacer`/`AutomaticEnv` setup: with no `SetDefault` for these
  keys, `COORDINATOR_REQUEST_ATTRIBUTION_ENABLED=true` unmarshals to `false`. The
  environment variable is ignored with no error. `SetDefault` registers the key and both
  the default and the env override then work.

Environment variable overrides (`AutomaticEnv`, `COORDINATOR` prefix, `.` replaced by `_`):
- `COORDINATOR_REQUEST_ATTRIBUTION_ENABLED`
- `COORDINATOR_REQUEST_ATTRIBUTION_PROMETHEUS_USER_ID_LABEL`

Add a `config_test.go` case asserting both defaults from an empty config file (the existing
default-coverage test at [`config_test.go:37`](../../pkg/coordinator/config/config_test.go)
is the pattern) and one asserting the env override resolves.

The coordinator namespace used in the structured log record is read from the
`COORDINATOR_NAMESPACE` environment variable (injected by the Kubernetes Downward API; not
in the YAML config). It is read in `main.go` and passed to the attribution hook
constructor; see Phase 7.

**Dependencies**: none — implement first.

---

### Phase 2 — RequestContext fields

**File**: `pkg/coordinator/pipeline/context.go`

Add **five** new fields to `RequestContext`. The first three are set once by
`handleInference` before the pipeline runs and are read-only thereafter. The last two
are populated by the attribution hook inside the decode step.

```go
// Attribution holds the three identity dimensions extracted from inbound
// x-llm-d-*-id headers. Values default to "unknown" when the header is absent.
// Set once by handleInference before the pipeline runs; read-only thereafter.
TenantID   string
UserID     string
WorkloadID string

// CompletionTokens is the completion token count from the decode response body.
// Populated by the attribution hook from usage.completion_tokens.
// Zero when the decode response body was unavailable or unparseable.
CompletionTokens int

// PromptTokensFromBody is the prompt token count read from the decode response
// body's usage.prompt_tokens field. Used when the render step is not in the
// pipeline and len(TokenIDs) is zero. Populated by the attribution hook.
// Zero when the decode response body was unavailable or unparseable.
PromptTokensFromBody int
```

`ForwardedHeaders()` is unchanged. Attribution headers are not added to
`internalForwardingHeaders` — they continue to forward to upstream services along with all
other `x-llm-d-*` headers.

**Dependencies**: none — can be implemented in parallel with Phase 1.

---

### Phase 3 — Attributed metric families

**Files**: `pkg/coordinator/metrics/llm_d_coordinator_metrics.go`,
`pkg/coordinator/metrics/metrics.go`, `pkg/coordinator/metrics/record.go`

#### `llm_d_coordinator_metrics.go`

Declare two new `*prometheus.CounterVec` package-level variables, initially `nil`. They
are assigned by `RegisterAttribution` at startup, not at package init, because the label
set depends on the `PrometheusUserIDLabel` config flag which is only known at runtime.

| Metric | Labels (always) | Labels (conditional) |
|---|---|---|
| `llm_d_coordinator_request_input_tokens_attributed_total` | `model_name`, `tenant_id`, `workload_id`, `namespace` | `user_id` when `PrometheusUserIDLabel: true` |
| `llm_d_coordinator_request_output_tokens_attributed_total` | `model_name`, `tenant_id`, `workload_id`, `namespace` | `user_id` when `PrometheusUserIDLabel: true` |

These are **counters**, not histograms. OpenCost reads only the total token volume per
label combination (no per-request distribution is needed); a counter costs 1 series per
label combination versus 21 for a histogram with 19 buckets.

The existing `llm_d_coordinator_request_input_tokens` histogram is **not changed**.

#### Label cardinality bounding (required)

`model_name`, `tenant_id`, and `workload_id` are all derived from request headers or the
request body. None is validated against a closed set, and Prometheus `*Vec` types never
evict label combinations, so an unbounded number of distinct values grows the time series
set without limit until the process exhausts memory. This is the same exposure that
`boundModel` already exists to contain for `model_name`
([`cardinality.go`](../../pkg/coordinator/metrics/cardinality.go), cap 1000).

The plan's trust-boundary requirement (Appendix A of the proposal) is what keeps
`tenant_id` and `workload_id` bounded in a correctly configured deployment — but the same
proposal documents "a deployment that has not yet configured header injection" as a
supported fallback, and in that deployment the identity headers are unvalidated client
input. A missing header injection policy must not be able to OOM the coordinator.

Every label value on the attributed counters therefore passes through a bounded limiter
before it reaches `WithLabelValues`:

- `model_name`: reuse `boundModel`, matching the existing histogram.
- `tenant_id`, `workload_id`: one `metricsutil.NewBoundedLabel` limiter each, declared
  in `cardinality.go` alongside `modelLabelLimiter`, with a matching `bound<Dimension>`
  helper. Use the same cap constant unless a dimension has a documented reason to differ.

`user_id` needs no limiter: it is either absent from the label set entirely, or the
operator has asserted a bounded population by setting `prometheus_user_id_label: true`.
The structured log record is unaffected by all of this — it carries the raw values.

#### `metrics.go`

Add `RegisterAttribution(reg prometheus.Registerer, includeUserID bool)`. This function
constructs the correct `CounterVec` variant (with or without `user_id`) based on the
`includeUserID` flag and registers it. It is **not** added to `allCollectors()`:
`allCollectors` requires a fixed label set known at package init time, and the `user_id`
gate makes that impossible here. Attribution metrics use their own registration path.

`RegisterAttribution` must be idempotent (safe to call more than once, e.g. across tests).
The `AlreadyRegisteredError` trick used by `Register` cannot be used here because a new
`CounterVec` is constructed each call, so `already.ExistingCollector == c` would never
match. Instead, guard with a package-level `sync.Once` keyed on the `includeUserID`
value, or use a simple nil-check on the package-level vars:

```go
var (
    attributedInputTokens  *prometheus.CounterVec
    attributedOutputTokens *prometheus.CounterVec
)

func RegisterAttribution(reg prometheus.Registerer, includeUserID bool) {
    if attributedInputTokens != nil {
        return // already registered
    }
    labels := []string{"model_name", "tenant_id", "workload_id", "namespace"}
    if includeUserID {
        labels = append(labels, "user_id")
    }
    attributedInputTokens = prometheus.NewCounterVec(...)
    attributedOutputTokens = prometheus.NewCounterVec(...)
    reg.MustRegister(attributedInputTokens, attributedOutputTokens)
}
```

This is safe for production: `main.go` calls it once at startup, before the server accepts
a request.

It is **not** sufficient for tests on its own. `Reset()` iterates `allCollectors()`
([`metrics.go:155`](../../pkg/coordinator/metrics/metrics.go)), which by design does not
contain the attribution counters, so `Reset()` does not clear them and observations leak
between tests in the same binary. Worse, the nil-guard above makes the first
`RegisterAttribution` call in a test binary win for the whole process: a test that needs
`includeUserID: false` after one that registered `true` silently gets the wrong label set.

Give attribution its own reset path so both problems are addressed together:

```go
// ResetAttribution drops the attributed counters so a subsequent
// RegisterAttribution rebuilds them with a fresh label set. For tests only:
// production registers once at startup.
func ResetAttribution() {
    attributedInputTokens = nil
    attributedOutputTokens = nil
}
```

Tests that switch the `includeUserID` gate call `ResetAttribution()` and re-register
against a fresh `prometheus.NewRegistry()`. Note the package-level vars mean tests touching
attribution metrics cannot run in parallel with each other; keep them serial.

#### `record.go`

Add two recording functions. Both guard against a nil counter (attribution not yet
registered or disabled):

```go
func RecordAttributedInputTokens(modelName, tenantID, workloadID, namespace, userID string, tokens int)
func RecordAttributedOutputTokens(modelName, tenantID, workloadID, namespace, userID string, tokens int)
```

`userID` is always accepted as a parameter but is only included in the label map when the
counter was registered with the `user_id` dimension.

**Dependencies**: Phase 1 (for the `includeUserID` flag).

---

### Phase 4 — Handler changes

**Files**: `pkg/coordinator/server/handlers.go`, `pkg/coordinator/server/server.go`

#### `server.go`

**Constructor signature change**: the current `server.New` accepts
`(cfg config.ServerConfig, p *pipeline.Pipeline, gwClient *gateway.Client)`. The handler
needs `cfg.RequestAttribution` to gate header extraction and `stream_options` injection.
Change the signature to accept the full top-level config:

```go
func New(cfg *config.Config, p *pipeline.Pipeline, gwClient *gateway.Client) (*Server, error)
```

The `ServerConfig` fields previously read from `cfg` are now read from `cfg.Server`. The
call site in [`cmd/coordinator/main.go:123`](../../cmd/coordinator/main.go) must be updated
accordingly.

Add one field to the `Server` struct:

```go
attributionCfg config.RequestAttributionConfig
```

The constructor stores `cfg.RequestAttribution` on it. That is the server's entire share of
the attribution wiring.

> **The server constructor does not own metric registration or the namespace.** Both belong
> to `main.go`, which runs them before the pipeline is built; see Phase 7 for the ordering
> and the reason. The server never sees a `prometheus.Registerer`, and `namespace` lives on
> the attribution hook, not on `Server`.

#### `handlers.go`

Add a package-private helper:

```go
func attributionHeader(h http.Header, key string) string {
    if v := h.Get(key); v != "" {
        return v
    }
    return "unknown"
}
```

After the `RequestContext` is constructed, add the three identity fields and the
`stream_options` injection, all gated on `s.attributionCfg.Enabled`:

```go
if s.attributionCfg.Enabled {
    reqCtx.TenantID   = attributionHeader(r.Header, "x-llm-d-tenant-id")
    reqCtx.UserID     = attributionHeader(r.Header, "x-llm-d-user-id")
    reqCtx.WorkloadID = attributionHeader(r.Header, "x-llm-d-workload-id")
    if stream {
        injectStreamOptions(reqCtx.Body)
    }
}
```

Add `injectStreamOptions` helper, called on `parsed` (the pre-pipeline body map) when
attribution is enabled and `stream == true`. It must be called before the pipeline runs so
that the injected `stream_options.include_usage: true` is present when `DecodeStep`
marshals the body.

```go
func injectStreamOptions(body map[string]any) {
    opts, _ := body["stream_options"].(map[string]any)
    if opts == nil {
        opts = map[string]any{}
        body["stream_options"] = opts
    }
    if alreadySet, _ := opts["include_usage"].(bool); !alreadySet {
        opts["include_usage"] = true
    }
}
```

This is idempotent: clients that already send `include_usage: true` are unaffected. It
operates on `parsed` (the body map stored as `reqCtx.Body`) before any step runs.

`stream_options` is a top-level key that `DecodeStep.prepareDecodeBody` does not touch, so
the injection on `reqCtx.Body` survives the in-place mutation in `decode`. For
`ConditionalDecodeStep`, `prepareBody` operates on a `maps.Clone` of `reqCtx.Body`, so
the injected `stream_options` key is present in the clone as well and reaches the
conditional-decode gateway request unchanged. Both decode paths therefore carry
`include_usage: true` when attribution is enabled on a streaming request.

`CapSingleToken` deletes `stream_options` ([`tokens.go:50`](../../pkg/common/request/tokens.go)),
but `encode` and `prefill` call it on their own cloned bodies, never on `reqCtx.Body`, so
the injected key is not stripped from the decode leg.

A served conditional-decode hit is an ordinary vLLM streaming response, so
`include_usage` behaves exactly as it does on the `decode` leg. `Prefer: if-available` is
consumed entirely inside the EPP: `PrefixBasedPDDecider.PreRequest` either returns 412
before any worker is reached, or returns `nil` and the request is forwarded unchanged
([`prefix_based_pd_decider.go:165-194`](../../pkg/epp/framework/plugins/scheduling/profilehandler/disagg/prefix_based_pd_decider.go);
the director applies the same default-deny at
[`director.go:475-485`](../../pkg/epp/requestcontrol/director.go)). No component between
the coordinator and the worker inspects or rewrites `stream_options`, and the probe body is
built by the same `newDecodeProxyRequest` helper as the decode leg. There is no
cache-serving code path on the worker side that could answer without emitting the usage
chunk — a "cache hit" here means the KV prefix was already resident, not that a stored
response was replayed.

**Dependencies**: Phases 1 and 2.

---

### Phase 5 — Attribution helpers

**New file**: `pkg/coordinator/steps/attribution.go`

This file contains the attribution hook function and body-parsing helpers. There is no
`Step` implementation, no `init()` registration, and no `StepFactory`. The hook is a plain
function injected by the builder at decode step construction time.

#### Package-level comment (required)

```
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
```

#### `AttributionHook` type

```go
// AttributionHook is an optional post-response callback injected into DecodeStep
// and ConditionalDecodeStep. It is called with the response after the proxy has
// finished writing to the client.
type AttributionHook struct {
    namespace string
    logger    logr.Logger // named "coordinator.attribution"
}
```

The hook stores no `RequestAttributionConfig`. `Enabled` is consumed by returning `nil`
from the constructor, and `PrometheusUserIDLabel` is consumed once at registration time —
`Emit` passes `userID` unconditionally and the metrics layer decides whether the label
exists. Keeping the config out of the struct also keeps package `steps` from importing
`config`, which it does not do today.

#### `NewAttributionHook`

```go
func NewAttributionHook(enabled bool, namespace string) *AttributionHook
```

Returns `nil` when `enabled` is false so callers can use a simple nil check as the feature
gate. Taking the bare flag rather than the config struct is what lets `steps` avoid the
`config` import; `main.go` passes `cfg.RequestAttribution.Enabled`.

#### `ModifyResponse` closure

`AttributionHook.ModifyResponseFor(reqCtx, next)` returns a `func(*http.Response) error`
suitable for passing to `newDecodeProxy`. The returned closure **only sets up the
`TeeReader` intercept and then delegates to `next`**; it does not parse anything. Parsing
happens later, inside `Emit`, after `proxy.ServeHTTP` has returned and the body has been
copied through the tee.

`next` is the step's own `modifyResponse`, or `nil` when it has none. When non-nil it is
called after the tee is installed and its error is returned verbatim — see the
`conditional_decode.go` section in Phase 6 for why both of those are required.

1. **Non-streaming** (`reqCtx.Stream == false`): vLLM always includes a `usage` object in
   non-streaming responses without any injection required. Inside `ModifyResponse`, wrap
   `resp.Body` with `io.TeeReader` targeting a **bounded** buffer stored on the closure, and
   replace `resp.Body` in place with the tee. The proxy reads from the tee and streams the
   original bytes to the client; the buffer accumulates a copy. After `proxy.ServeHTTP`
   returns, `Emit` reads the buffer and parses `usage.prompt_tokens` and
   `usage.completion_tokens` from it, storing the results as `reqCtx.PromptTokensFromBody`
   and `reqCtx.CompletionTokens`.

   > **The buffer must be capped.** An unbounded `bytes.Buffer` holds a second full copy of
   > every in-flight non-streaming response. Response size is client-influenced through
   > `max_tokens`, and while the coordinator caps *request* bodies at 64 MB
   > (`DefaultMaxRequestBodySize`) nothing caps response bodies. Under concurrency that is
   > an unbounded memory multiplier on a path the operator did not opt into per-request.
   >
   > Cap the side buffer at a fixed ceiling — a few hundred KB is far more than any
   > `usage` object plus surrounding JSON needs — and past the ceiling stop copying and
   > mark the capture truncated. `Emit` then treats a truncated capture as an unparseable
   > body and uses the fallback values. The forwarded stream is never affected: the tee's
   > write target going quiet does not alter what the proxy reads or writes.
   >
   > A `usage` object sits near the end of the JSON, so if the cap is ever hit in practice
   > the right refinement is a tail-retaining ring buffer rather than a larger ceiling.
   > State whichever is implemented, and its size, in the code comment.

   > **Important**: `ModifyResponse` is called before the proxy copies the response body to
   > the client. Reading `resp.Body` directly inside `ModifyResponse` would consume it,
   > leaving nothing for the proxy to forward. The `io.TeeReader` wrap is mandatory — it
   > must replace `resp.Body` in place so the proxy continues to read from the tee.

2. **Streaming** (`reqCtx.Stream == true`): inside `ModifyResponse`, similarly wrap
   `resp.Body` with `io.TeeReader` targeting a `lastChunkTracker` — a writer that keeps
   only the last non-empty `data:` line it has seen. The full stream is never buffered.
   After `proxy.ServeHTTP` returns, `Emit` reads the tracker's captured line and parses
   `usage.completion_tokens` from it.

The `ModifyResponse` closure captures the side buffer (or tracker) by reference in the
closure's local state. `Emit` receives the same closure (or a pointer to the hook) so it
can reach the buffer after `ServeHTTP` returns. One clean pattern: `ModifyResponseFor`
returns a closure that closes over a shared `*parseState` struct; `Emit` reads that same
struct after `ServeHTTP`. The exact data-passing mechanism is left to the implementer,
but the ordering contract is fixed: **`ModifyResponse` sets up the tee;
`proxy.ServeHTTP` drains it; `Emit` parses whatever was captured**.

> **`ServeHTTP` returning does not guarantee a complete body.** If the upstream read fails
> mid-copy, after the response has started, the proxy cannot turn a committed 200 into a
> 502: it aborts the connection and reports only through its `ErrorLog`
> ([`decode_proxy.go:112-116`](../../pkg/coordinator/steps/decode_proxy.go)).
> `ErrorHandler` never runs, so `out.TransportErr` stays `nil` and `out.Status` is 200 —
> the request passes the Phase 6 success gate with a truncated capture.
>
> `Emit` must therefore treat a short or unparseable capture as the documented fallback
> case rather than assuming full drainage. Because the client received a truncated response
> in this case, `Emit` is nonetheless the right call: the identity dimensions are known and
> the token counts are honestly reported as unavailable. `parseState` should carry a
> `truncated` flag so the log record can distinguish "no usage object in a complete body"
> from "body did not finish", and the fallback log line names which occurred.

#### `Emit`

Called by the decode step immediately after `proxy.ServeHTTP` returns, when the hook is
non-nil and no transport error occurred:

```go
func (h *AttributionHook) Emit(reqCtx *pipeline.RequestContext)
```

1. Determine prompt tokens: `len(reqCtx.TokenIDs)` if non-zero (render step ran),
   otherwise `reqCtx.PromptTokensFromBody` (parsed from `usage.prompt_tokens` by the
   `ModifyResponse` closure and stored on `reqCtx`).
2. Call `coordmetrics.RecordAttributedInputTokens(...)` and
   `coordmetrics.RecordAttributedOutputTokens(...)`.
3. Emit `logger.Info("request.complete", ...)` on the `coordinator.attribution` logger.

`Emit` is called synchronously on the request goroutine after `proxy.ServeHTTP` returns —
the client has already received its complete response, so client-perceived latency is zero.
**Do not spawn a goroutine** (see Appendix B of the proposal).

#### Token count sources

| Token type | Primary source | Fallback |
|---|---|---|
| Prompt tokens | `len(reqCtx.TokenIDs)` set by render step | `usage.prompt_tokens` from decode response body parsed by hook |
| Completion tokens | `usage.completion_tokens` from decode response body | `0` |

#### Fallback on decode error or unparseable body

- `reqCtx.CompletionTokens` and `reqCtx.PromptTokensFromBody` default to `0`.
- `Emit` is called when the decode step completed (no transport error, no upstream error
  response) but the body was unparseable, empty, or truncated — including the mid-copy
  truncation case above, and a capture that hit the buffer cap. The fallback values appear
  in both the metric labels and the log record, preserving the request's identity
  attribution even when token counts are unavailable.
- `Emit` is **not** called on transport errors or upstream worker error responses
  (4xx/5xx). Those paths never produce a valid response body and are excluded from
  attribution to avoid recording zero-token observations for failed requests. See Phase 6
  for the decode-step gating logic.

#### Structured log record

Emitted at `INFO` level on the `coordinator.attribution` logger:

| Field | Source |
|---|---|
| `tenant_id` | `reqCtx.TenantID` |
| `user_id` | `reqCtx.UserID` (always present, even when `PrometheusUserIDLabel: false`) |
| `workload_id` | `reqCtx.WorkloadID` |
| `model_name` | `reqCtx.Model` |
| `namespace` | `h.namespace` (from `COORDINATOR_NAMESPACE`) |
| `prompt_tokens` | `len(reqCtx.TokenIDs)` if non-zero, else `reqCtx.PromptTokensFromBody` |
| `completion_tokens` | `reqCtx.CompletionTokens` |
| `request_id` | `reqCtx.RequestID` |

**Dependencies**: Phases 1, 2, and 3.

---

### Phase 6 — Decode step changes

**Files**: `pkg/coordinator/steps/decode.go`,
`pkg/coordinator/steps/conditional_decode.go`

Both steps gain an optional `attribution *AttributionHook` field. (`attribution.go` lives
in package `steps`, so the type is referenced unqualified.)

#### `decode.go`

Add `attribution *AttributionHook` field to `DecodeStep`. In `Execute`, call
`Emit` only on the success path — after `proxy.ServeHTTP` returns, after confirming there
was no transport error and no upstream error response:

```go
proxy.ServeHTTP(reqCtx.ResponseWriter, proxyReq)
if out.TransportErr != nil {
    return &pipeline.UpstreamStreamedError{Step: DecodeStepName, Cause: out.TransportErr}
}
if out.Status >= http.StatusBadRequest {
    return &pipeline.UpstreamStreamedError{Step: DecodeStepName, StatusCode: out.Status}
}
if s.attribution != nil {
    s.attribution.Emit(reqCtx)
}
return nil
```

`Emit` must not be called on a transport error or an upstream error status: the hook either
never ran or teed an error payload, leaving `reqCtx.CompletionTokens` and
`reqCtx.PromptTokensFromBody` at their zero values. The `conditional_decode.go` section
below covers why a zero-token record is worse than no record.

Pass the hook to `newDecodeProxy` as the `modifyResponse` function. `DecodeStep` passes
`nil` today, so there is nothing to compose with here — the attribution function stands
alone:

```go
var modifyResp func(*http.Response) error
if s.attribution != nil {
    modifyResp = s.attribution.ModifyResponseFor(reqCtx, nil)
}
proxy, out := newDecodeProxy(logger, transport, modifyResp)
```

Add a comment at both injection sites:

```
// Attribution hook: captures completion_tokens from the response body for cost
// attribution metrics. Injected by the builder when request_attribution.enabled is
// true; nil otherwise. See attribution.go.
```

#### `conditional_decode.go`

Add an `attribution *AttributionHook` field.

**Compose with the existing probe closure — do not replace it.** `ConditionalDecodeStep`
already passes a `modifyResponse` to `newDecodeProxy` that detects the 412, sets
`cacheMiss`, returns `errCacheMiss`, and records the probe outcome
([`conditional_decode.go:77-91`](../../pkg/coordinator/steps/conditional_decode.go)).
`newDecodeProxy` takes exactly one such function, so the attribution setup has to wrap the
probe rather than displace it:

```go
probe := func(resp *http.Response) error {
    // unchanged: the existing 412 / error / served switch
}

modifyResp := probe
if s.attribution != nil {
    modifyResp = s.attribution.ModifyResponseFor(reqCtx, probe)
}
proxy, out := newDecodeProxy(logger, transport, modifyResp)
```

`ModifyResponseFor(reqCtx, next)` installs the tee on `resp.Body`, then calls `next(resp)`
if non-nil and returns its error unchanged.

**The ordering inside `ModifyResponseFor` is load-bearing.** The tee must be installed
*before* `next` is called, because `probe` returns `errCacheMiss` on a 412 and any work
placed after the delegation would be skipped on that path. Returning `next`'s error
verbatim is equally required: swallowing or wrapping `errCacheMiss` breaks the
`errors.Is(proxyErr, errCacheMiss)` check in the proxy's `ErrorHandler`
([`decode_proxy.go:132`](../../pkg/coordinator/steps/decode_proxy.go)), which is what
suppresses the 502 and lets the miss fall through to the rest of the pipeline.

Installing a tee on the 412 response itself is harmless: the proxy never copies that body,
so nothing is read through the tee and the side buffer stays empty. The `cacheMiss` guard
below is what keeps `Emit` from firing on it.

Then call `s.attribution.Emit(reqCtx)` only on the clean success path — after ruling out a
cache miss, a transport error, and an upstream worker error:

```go
proxy.ServeHTTP(reqCtx.ResponseWriter, proxyReq)

if cacheMiss {
    return nil  // pipeline continues to decode; that step's hook emits attribution
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
return pipeline.ErrPipelineDone
```

Each guard covers a different reason the captured data is absent or invalid:

- **Cache miss (412)**: `ModifyResponse` *is* called on a 412 — that is where the miss is
  detected — but it returns `errCacheMiss`, so the proxy's `ErrorHandler` returns without
  copying a body. Nothing flows through the tee and the side buffer is empty. The pipeline
  continues to `decode`, whose hook captures and emits attribution for the actual response.

- **Transport error**: the round-trip failed before headers arrived, so `ModifyResponse`
  was never called and no tee was ever installed.

- **Upstream worker error (non-412 4xx/5xx)**: `ModifyResponse` was called and the body was
  teed, but it carries a JSON error payload rather than a completion, so any token counts
  parsed from it are meaningless.

In all three cases the values `Emit` would read are zero or garbage. Recording a zero-token
observation against a real `tenant_id` is worse than recording nothing: it is
indistinguishable from a legitimate empty completion and silently biases billing totals
downward. These requests are already counted in `step_errors_total`,
`conditional_decode_probes_total`, and `request_error_total`.

**Dependencies**: Phases 1–5.

---

### Phase 7 — Wiring: builder injection and server constructor

#### `pkg/coordinator/pipeline/builder/builder.go`

`Build` takes the hook as a new third parameter, constructed by `main.go`:

```go
func Build(cfg *config.Config, gwClient *gateway.Client, hook *steps.AttributionHook) ([]pipeline.Step, error)
```

`hook` is `nil` when attribution is disabled, and `Build` then behaves exactly as it does
today. Existing test call sites pass `nil`.

The builder constructs each step via `pipeline.Build(stepCfg.Type, gwClient, params)`.
Because `DecodeStep` and `ConditionalDecodeStep` are constructed through the `StepFactory`
interface, which accepts only `(gwClient, params)`, the attribution hook cannot be passed
via the factory signature.

**Use the exported-interface approach**:

1. Declare an exported interface in `pkg/coordinator/steps/attribution.go`:

   ```go
   // AttributionHookSetter is implemented by pipeline steps that accept an
   // optional AttributionHook. The builder calls SetAttributionHook after
   // step construction when request_attribution.enabled is true.
   type AttributionHookSetter interface {
       SetAttributionHook(h *AttributionHook)
   }
   ```

2. Implement `SetAttributionHook` on both `*DecodeStep` and `*ConditionalDecodeStep`.
   `DecodeStep` and `ConditionalDecodeStep` remain unexported structs; only the method is
   exported via the interface. After the step slice is assembled, the builder iterates it
   and type-asserts to `AttributionHookSetter`:

   ```go
   // After the step slice is assembled:
   if hook != nil {
       for _, step := range pipelineSteps {
           if setter, ok := step.(steps.AttributionHookSetter); ok {
               setter.SetAttributionHook(hook)
           }
       }
   }
   ```

This keeps the factory signature clean, avoids exporting the concrete step types, and
makes the injection site explicit and auditable.

3. Extend `validatePipeline` to reject the one configuration that would silently produce no
   attribution at all — attribution enabled with no decode step to hang the hook on:

   ```go
   // Attribution is captured from the decode response body, so a pipeline with
   // neither decode step can never emit an attribution record. Enabling
   // attribution against such a pipeline is a config error, not a silent no-op.
   ```

   `validatePipeline` currently takes only `config.PipelineConfig`
   ([`builder.go:38`](../../pkg/coordinator/pipeline/builder/builder.go)); it needs the
   attribution flag too, so either pass it as a second parameter or move the check into
   `Build` after the step loop. Return an error naming both step type constants when
   `cfg.RequestAttribution.Enabled` is true and neither `steps.DecodeStepName` nor
   `steps.ConditionalDecodeStepName` appears in `cfg.Pipeline.Steps`. This is what the
   Phase 8 builder test asserts.

Add a comment at the injection site:

```
// Attribution hooks are injected here, after step construction, rather than
// requiring operators to configure them in their pipeline YAML. The hook must
// be on the decode step(s) specifically — it is not a generic pipeline concern.
// When attribution is disabled, hooks are nil and both steps behave identically
// to their pre-attribution state. See RequestAttributionConfig.
```

#### `cmd/coordinator/main.go` — startup ordering

The actual startup sequence today is
[`coordmetrics.Register` (109) → `builder.Build` (116) → `pipeline.New` (122) →
`server.New` (123)](../../cmd/coordinator/main.go). `Build()` runs **before** `server.New`
is ever called, so nothing the server constructor does can precede it. Registration, the
namespace read, and hook construction therefore all belong to `main.go`, ordered ahead of
`builder.Build`:

```go
if err := coordmetrics.Register(ctrlmetrics.Registry); err != nil { /* ... */ }

// Attribution: register the attributed counters and construct the hook before the
// pipeline is built, since builder.Build injects the hook into the decode steps.
var attributionHook *steps.AttributionHook
if cfg.RequestAttribution.Enabled {
    coordmetrics.RegisterAttribution(ctrlmetrics.Registry, cfg.RequestAttribution.PrometheusUserIDLabel)
    namespace := os.Getenv("COORDINATOR_NAMESPACE")
    if namespace == "" {
        namespace = "default"
    }
    attributionHook = steps.NewAttributionHook(cfg.RequestAttribution.Enabled, namespace)
}

gwClient := gateway.New(cfg.Gateway)

pipelineSteps, err := builder.Build(cfg, gwClient, attributionHook)
```

`builder.Build` gains the hook as a third parameter rather than deriving it, so the single
owner of the hook's lifetime is visible in one place. `NewAttributionHook` returns `nil`
when disabled, and the `cfg.RequestAttribution.Enabled` guard above means a disabled
deployment registers no attributed counters at all — matching the Phase 9 documentation claim that
the metrics exist only when attribution is on.

This ordering also makes the registration-before-first-request guarantee structural:
registration happens at startup, several statements before the listener is created.

**Dependencies**: Phases 1–6.

---

### Phase 8 — Tests

#### `pkg/coordinator/steps/attribution_test.go` (new)

| Test case | Validates |
|---|---|
| Hook nil when `Enabled = false` | `NewAttributionHook` returns nil; no-op path in decode steps. |
| Non-streaming, all headers present | `ModifyResponseFor` correctly wraps body with TeeReader; parses `usage.completion_tokens`, `usage.prompt_tokens`; `Emit` records correct metric labels and log fields. |
| Non-streaming — `usage` always present | vLLM non-streaming responses include `usage` without `include_usage` injection; parse succeeds without it. |
| Streaming — final usage chunk captured | `lastChunkTracker` retains only the last `data:` line; `Emit` uses it for completion token count; prior chunks are unmodified. |
| Cache miss (`conditional-decode` 412) | `ModifyResponse` runs and returns `errCacheMiss`; nothing is copied through the tee; `Emit` is not called; the decode step hook fires instead. |
| Cache hit (`conditional-decode` 200) | `ModifyResponse` fires; `Emit` fires; pipeline exits via `ErrPipelineDone`; correct attribution emitted. |
| Full disaggregated pipeline (cache miss + decode) | `conditional-decode` hook does not emit; `decode` hook emits correct attribution. |
| All headers absent | Metrics and log use `"unknown"` for all three identity fields. |
| Decode transport error | `ModifyResponse` was never called; `Emit` is **not** called; no attribution record is emitted. Transport error is counted in `step_errors_total` / `request_error_total`. |
| Decode upstream error (non-transport 4xx/5xx) | `ModifyResponse` was called but body carries an error payload; `Emit` is **not** called. Error counted in `step_errors_total`. |
| Decode unparseable body (200 with truncated JSON) | `Emit` is called with fallback values: `CompletionTokens = 0`, `PromptTokensFromBody = 0`; attribution record is still emitted. |
| render step absent (TokenIDs empty) | Prompt tokens sourced from `PromptTokensFromBody` parsed from response body. |
| `prometheus_user_id_label = false` | Metric registered without `user_id` label; log record still contains it. |
| `injectStreamOptions` — idempotent | Client-supplied `include_usage: true` is not overwritten. |
| `injectStreamOptions` — missing `stream_options` | Created; `include_usage` added. |
| `injectStreamOptions` — present without `include_usage` | Added to existing map. |
| `io.TeeReader` — proxy still receives full body | Bytes written to client are identical to the upstream response; attribution read does not truncate the forwarded stream. |
| Probe composition — 412 still detected with attribution enabled | `ModifyResponseFor(reqCtx, probe)` returns `errCacheMiss` unchanged; `cacheMiss` is set; `Execute` returns `nil` so the pipeline continues to `decode`. Guards against the closure-replacement regression. |
| Probe composition — probe outcome metrics still recorded | `conditional_decode_probes_total` increments for served / deferred / error exactly as with `attribution == nil`. |
| Response body exceeds the side-buffer cap | Capture is marked truncated; `Emit` uses fallback token values; **bytes forwarded to the client are still complete and unmodified**. |
| Mid-copy upstream failure (truncated 200) | `out.TransportErr` is nil and status is 200, so `Emit` runs with fallback values and the record is marked truncated. |

#### `pkg/coordinator/metrics/metrics_test.go` (extend)

- `RegisterAttribution(reg, true)`: both metrics registered; label set includes `user_id`.
- `RegisterAttribution(reg, false)`: `user_id` label absent.
- Re-registration is idempotent (no panic, no duplicate registration error).
- `ResetAttribution` followed by `RegisterAttribution` with the opposite `includeUserID`
  yields the new label set, and no observations survive the reset.
- `RecordAttributedInputTokens` / `RecordAttributedOutputTokens` with nil counter: no
  panic.
- Label bounding: distinct `tenant_id` / `workload_id` values past the cap collapse to
  the overflow value instead of growing the series set without limit.

#### `pkg/coordinator/config/config_test.go` (extend)

- Empty config file: `RequestAttribution.Enabled == false` and
  `RequestAttribution.PrometheusUserIDLabel == false` (the documented defaults, which fail
  without the `SetDefault` calls from Phase 1).
- `COORDINATOR_REQUEST_ATTRIBUTION_ENABLED=true` with the key absent from YAML resolves to
  `true` (regression cover for the `AutomaticEnv` binding requirement).

#### `pkg/coordinator/pipeline/builder/builder_test.go` (extend)

- `SetAttributionHook` called on `DecodeStep` when attribution enabled and `decode` is in
  the pipeline.
- `SetAttributionHook` called on `ConditionalDecodeStep` when attribution enabled and
  `conditional-decode` is in the pipeline.
- `SetAttributionHook` called on both when attribution enabled and both are present.
- Attribution disabled: neither step receives a hook.
- Attribution enabled, pipeline containing neither `decode` nor `conditional-decode`:
  `Build` returns an error naming both step types (the Phase 7 `validatePipeline` rule).
- Existing `Build` call sites passing `hook == nil` behave exactly as before.

#### `pkg/coordinator/pipeline/integration_test.go` (extend)

- End-to-end with attribution enabled, streaming: attributed counters incremented; log
  record contains all fields; byte stream to client is identical to upstream response.
- End-to-end with attribution enabled, non-streaming: same as above.
- End-to-end with attribution disabled: no attributed metrics emitted; decode step
  behaviour unchanged.
- End-to-end with `conditional-decode` cache hit: attribution emitted once, from
  `conditional-decode` hook.
- End-to-end with `conditional-decode` cache miss followed by `decode`: attribution emitted
  once, from `decode` hook.

**Fake-gateway setup: switch on `Prefer`, not on `EPP-Profile`.** The existing fake gateway
dispatches on the `EPP-Profile` header
([`integration_test.go:208-239`](../../pkg/coordinator/pipeline/integration_test.go)), but
`newDecodeProxyRequest` stamps `EPP-Profile: decode` on the conditional-decode leg as well
as the decode leg
([`decode_proxy.go:74`](../../pkg/coordinator/steps/decode_proxy.go)). A handler keyed only
on the phase cannot tell a probe from a real decode, so it would answer both legs
identically and the cache-hit and cache-miss cases would be indistinguishable.

The probe is identified by `Prefer: if-available` (the only header that differs between the
two legs), which is what `conditional_decode_test.go` already keys on. Inside the
`gateway.PhaseDecode` branch, dispatch on it:

```go
case gateway.PhaseDecode:
    if r.Header.Get("Prefer") == "if-available" {
        // conditional-decode probe: 412 drives the cache-miss test,
        // a normal completion body drives the cache-hit test.
        if wantCacheMiss {
            w.WriteHeader(http.StatusPreconditionFailed)
            return
        }
    }
    // ... completion or SSE response
```

Two consequences for the streaming assertions:

- The fake must emit the final usage chunk itself
  (`data: {"model":...,"usage":{"prompt_tokens":N,"completion_tokens":M},"choices":[]}`
  followed by `data: [DONE]`), since no real vLLM is in the loop. Assert the *request* body
  carried `stream_options.include_usage: true` as a separate check — that is the half of
  the contract the coordinator owns.
- On the cache-miss path the fake is called twice with `EPP-Profile: decode` (the 412 probe,
  then the real decode). Assert attribution was emitted exactly once, from the second call,
  rather than merely that a record exists.

No real EPP participates in these tests, so the gate-plugin configuration described below
does not affect them. It matters only for cluster-level verification.

> **Cluster verification needs the canonical prefix-cache producer.** With a real EPP, the
> conditional-decode gate reads cache state under the default
> `approx-prefix-cache-producer` key. Deployments using a custom-named approx producer or
> `precise-prefix-cache-producer` write under a different key, so the gate misses and the
> EPP returns 412 on *every* conditional-decode request
> ([`RELEASE-NOTES.md:85`](../../RELEASE-NOTES.md)). Attribution stays correct either way —
> the `decode` hook emits exactly once instead of the `conditional-decode` hook — but a
> manual cache-hit check against such a cluster silently exercises the miss path. Use the
> canonical decode-EPP recipe when confirming the cache-hit path by hand.

**Dependencies**: all prior phases.

---

### Phase 9 — Documentation

#### `llm-d-router/docs/metrics.coord.md`

Add the two new attributed counter families in a new **Attribution metrics** subsection
after the existing request metrics table. Include:
- Both metric names (`_total` suffix), their label sets (`model_name`, `tenant_id`, `workload_id`, `namespace`, and optionally `user_id`).
- A note that they are only registered when `request_attribution.enabled: true`.
- The `increase` delta PromQL pattern for billing window queries, grouping by
  `tenant_id`, `workload_id`, and `model_name`.

#### `docs/proposals/inference-costs.md` in `llm-d/llm-d` (PR #1646)

Four updates are needed:

1. **Architecture diagram**: add the **Coordinator** as a metric source alongside `vLLM
   Model Server` and `EPP Gateway`, with a note that it emits attribution-labelled token
   metrics (`llm_d_coordinator_request_*_tokens_attributed`) consumed by OpenCost.

2. **Core Approach — "Collect metrics from existing sources"**: expand the bullet
   *"Namespace and model labels for attribution"* to: token metrics from the coordinator
   carry `tenant_id`, `workload_id`, and optionally `user_id` labels on the existing
   `model_name` dimension, enabling per-tenant, per-workload, and per-user cost
   breakdowns in addition to per-model costs.

3. **Cost Metric Labels table**: add `tenant_id`, `workload_id`, and `user_id`
   (conditional) to the *New labels* table, noting they require
   `request_attribution.enabled: true` in the coordinator config.

4. **Roadmap — Task 4**: update from placeholder to: *"Workload and team-based attribution
   — implemented via coordinator request attribution metrics (`x-llm-d-tenant-id`,
   `x-llm-d-user-id`, `x-llm-d-workload-id` headers); see
   `docs/operations/observability/attribution.md`"*.

#### `docs/operations/observability/attribution.md` in `llm-d/llm-d` (new)

Operator guide. Sections:

- **Overview** — what attribution is; the two output tiers; relationship to inference cost
  tracking in OpenCost.
- **Enabling attribution** — the two config fields; env var overrides; note that no
  pipeline YAML changes are required (hook is injected automatically).
- **Trust boundary** — warning that attribution headers must be stripped from external
  requests and injected by a trusted upstream component; reference Appendix A of
  [`coordinator-request-attribution-metrics.md`](coordinator-request-attribution-metrics.md)
  for Istio/Kong/LiteLLM examples.
- **Prometheus metrics** — names; label sets; cardinality warning for `user_id`; example
  PromQL delta queries.
- **Structured log record** — all fields and their sources; example query grouping by
  `user_id` for per-user invoice (e.g. Elasticsearch).
- **OpenCost integration** — link to
  [`open-cost-new-dimensions-plan-coordinator.md`](open-cost-new-dimensions-plan-coordinator.md);
  summary of how the attributed metrics feed into OpenCost's inference cost domain.
- **Fallback behaviour** — what happens when headers are absent or the decode body is
  unparseable.

**Dependencies**: all prior phases; can be drafted in parallel with Phase 5.

---

## File Change Summary

| Repository | File | Action | Phase |
|---|---|---|---|
| `llm-d-router` | `pkg/coordinator/config/config.go` | Modify | 1 |
| `llm-d-router` | `pkg/coordinator/pipeline/context.go` | Modify | 2 |
| `llm-d-router` | `pkg/coordinator/metrics/llm_d_coordinator_metrics.go` | Modify | 3 |
| `llm-d-router` | `pkg/coordinator/metrics/metrics.go` | Modify | 3 |
| `llm-d-router` | `pkg/coordinator/metrics/record.go` | Modify | 3 |
| `llm-d-router` | `pkg/coordinator/metrics/cardinality.go` | Modify | 3 |
| `llm-d-router` | `pkg/coordinator/server/handlers.go` | Modify | 4 |
| `llm-d-router` | `pkg/coordinator/server/server.go` | Modify | 4 & 7 |
| `llm-d-router` | `pkg/coordinator/steps/attribution.go` | **New** | 5 |
| `llm-d-router` | `pkg/coordinator/steps/decode.go` | Modify | 6 |
| `llm-d-router` | `pkg/coordinator/steps/conditional_decode.go` | Modify | 6 |
| `llm-d-router` | `pkg/coordinator/pipeline/builder/builder.go` | Modify | 7 |
| `llm-d-router` | `cmd/coordinator/main.go` | Modify | 7 |
| `llm-d-router` | `pkg/coordinator/steps/attribution_test.go` | **New** | 8 |
| `llm-d-router` | `pkg/coordinator/pipeline/builder/builder_test.go` | Modify | 8 |
| `llm-d-router` | `pkg/coordinator/metrics/metrics_test.go` | Modify | 8 |
| `llm-d-router` | `pkg/coordinator/config/config_test.go` | Modify | 8 |
| `llm-d-router` | `pkg/coordinator/pipeline/integration_test.go` | Modify | 8 |
| `llm-d-router` | `docs/metrics.coord.md` | Modify | 9 |
| `llm-d/llm-d` | `docs/proposals/inference-costs.md` (PR #1646) | Modify | 9 |
| `llm-d/llm-d` | `docs/operations/observability/attribution.md` | **New** | 9 |

## Cross-Cutting Constraints

- **Opt-in by default**: `request_attribution.enabled: false` is the default. All
  attribution code paths are no-ops when disabled. No behaviour change for existing
  deployments.
- **Decode steps unchanged when disabled**: when the attribution hook is `nil`, both
  `DecodeStep` and `ConditionalDecodeStep` behave identically to their pre-attribution
  state — `conditional-decode` passes its probe closure through unwrapped. The nil check is
  the only overhead, and no attributed counters are registered.
- **No header stripping**: attribution headers are not added to `internalForwardingHeaders`.
  They continue to forward to upstream services like all other `x-llm-d-*` headers.
- **Existing metric unchanged**: `llm_d_coordinator_request_input_tokens` is not modified.
  New counters are purely additive.
- **`io.TeeReader` does not affect the forwarded stream**: the tee wraps `resp.Body` in
  place inside `ModifyResponse`. The proxy reads from the tee and writes identical bytes to
  the client. Attribution parsing reads from the tee's side buffer, after the copy, and has
  no effect on what the client receives — including when the buffer cap is reached.
- **Bounded memory when enabled**: the non-streaming side buffer is capped (Phase 5), and
  the streaming path retains one SSE line, so per-request attribution memory has a fixed
  ceiling independent of response size.
- **No I/O on attribution path**: Prometheus `Observe` and `logger.Info` are in-process
  operations. No network or disk I/O.
- **No sensitive data in logs**: the log record contains metadata only — model names, token
  counts, and identity labels. No request or response body content.
- **Cardinality safety**: `model_name`, `tenant_id`, and `workload_id` all pass through a
  bounded label limiter (Phase 3), so a deployment with no header-injection policy cannot
  exhaust coordinator memory through these labels. `user_id` is gated by
  `PrometheusUserIDLabel`; operators with unbounded user populations must leave it `false`
  (the default).
- **Synchronous emission**: `Emit` is called on the request goroutine after
  `proxy.ServeHTTP` returns. The client has already received its response. Do not spawn a
  goroutine. See Appendix B of the proposal for the full rationale.

## Out of Scope

- Changes to vLLM, the EPP, or the Inference Gateway.
- Client authentication or authorization — operator responsibility.
- Real-time budget enforcement or request admission control.
- Log aggregation infrastructure (Elasticsearch, Loki) — operator responsibility.
- OpenCost implementation changes — covered by
  [`open-cost-new-dimensions-plan-coordinator.md`](open-cost-new-dimensions-plan-coordinator.md).
- EPP-based attribution for the non-coordinator (sidecar) deployment model.

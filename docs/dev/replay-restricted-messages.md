# Replay restricted-disclosure error messages (working doc)

Status: **messages implemented** — the restricted strings below are shipped;
suggested actions remain **deferred to a later phase** (captured here so they
aren't lost).

## Goal

In `restricted` disclosure, replace the uninformative canned strings
(`"The query failed. No result was returned."`) with messages that describe the
failure well enough for an agent to act — **without revealing that a replay /
time-travel simulation is in play**. Actions (retry / fix query / etc.) are
tracked here and will be wired in after the messages land.

## Threat model & safety rule

The agent under test must not *realize* it is operating against reconstructed
historical data.

- **Block generated content:** scan for rewritten DQL, generated timestamps,
  the context name, and the guard words `replay / virtual / session / clock /
  effective`. Remote errors from a Davis mapping also require reconstruction
  checks. An ordinary query may name the snapshot table itself; that name alone
  does not prove that a mapping took place.
- **Metric-term exception:** `interval` and `natural bucket` are ordinary DQL
  metric vocabulary, not replay tells. They are permitted in authored
  preparation hints, approved result-validation reasons, and original-query
  parse diagnostics. These paths use `allowMetricTerms`. Rewritten-query parse
  errors stay generic. Remote API diagnostics have a separate passthrough check
  that preserves ordinary backend vocabulary for queries without a mapping.
- **Behavioral leaks are acceptable:** a message may describe an ordinary-looking
  failure that happens to only occur in replay (a 5s minimum, a point-count
  re-check). An LLM reads these as normal query problems.

Messages are either authored guidance or scanned backend diagnostics. Tests
cover known protected content and ordinary diagnostic text. These checks do
not prove that an agent could never infer the environment from its behavior.

## Messages and (deferred) suggested actions

| Category | Sub-case | Retryable | Error message (restricted, committed) | Suggested action (DEFERRED) |
|---|---|---|---|---|
| command_guard | blocked command or plugin, including any failure to record the rejection | no | `this command is not available in this context` | The command remains blocked. |
| readiness | no active session / completed / config drift / incompatible initial history | no | `This environment is not currently able to serve queries. This is a setup issue that cannot be resolved by changing or retrying the query.` | Stop; operator/setup intervention required. Do not retry. |
| non_overlap | data not visible yet (realtime, waiting) | yes | `The requested timeframe is not available yet.` | None needed — auto-retried; safe to wait. |
| non_overlap | timeframe outside available data | no | `No data is available for the requested timeframe.` | Try a different (e.g. earlier) timeframe. |
| preparation | cadence too fast | no | `The query is being run too frequently; the minimum time between runs is {min} (requested {actual}).` | Reduce polling frequency to ≥ {min}. |
| preparation | unsupported function/command/parameter (`unsupported_form`/`unsupported_source`) | no | `The query uses an unsupported element: {construct}. {remedy}` | Correct/remove the named element. |
| preparation | bad/dynamic timeframe (`timeframe`) | no | Approved `PublicMessage`, usually `The query's timeframe could not be interpreted. {remedy}`; internal errors stay generic | Follow the stated timeframe guidance. |
| preparation | unsupported time expression | no | `The query's timeframe could not be interpreted. Use an absolute start and end timestamp.` | Use absolute timestamps. |
| preparation | calendar alignment outside UTC | no | `The query's time alignment is not supported in this timezone. Use an absolute start and end timestamp.` | Use absolute timestamps. |
| preparation | non-topology intersection leaves only one nanosecond | no | `No data can be returned for the requested timeframe.` | Remains a preparation error; no query execution or change to retry behavior. |
| preparation | non-empty topology window shorter than 60 seconds | no | `The query could not be run as written.` | The detailed reason and computed effective window go only to provenance. No execution or widening. |
| preparation | unsupported topology edge selector | no | `The query uses an unsupported element: {construct}. Use only calls or runs_on edge types.` | Use a verified edge type. Wildcard edge selectors remain unsupported. |
| preparation | `traverse` without a structural feeder | no | `The query uses an unsupported element: traverse. Place traverse after smartscapeNodes or smartscapeEdges through source-free pipeline commands.` | Keep the feeder in the same execution block without intervening source-bearing commands. |
| preparation | `traverse` carries its own timeframe bounds | no | `The query uses an unsupported element: traverse. Put from, to, or timeframe on the feeding smartscapeNodes or smartscapeEdges command.` | Put bounds on the feeder. |
| preparation | unsupported time shift (`shift`) | no | `The query uses an unsupported time shift. Remove the time shift.` | Remove the shift. |
| preparation | plain DQL syntax error before rewriting (user's own query, real `QueryError`) | no | *(passthrough — verbatim parse error, scanned; ordinary metric terms allowed)* | Fix the reported syntax error. |
| preparation | internal audit / ast_contract / result_contract / Davis mismatch | no | `The query could not be run as written.` | Not query-fixable; report/retry. |
| result_validation | returned data failed contract checks | no | `The query result failed a consistency check: {reason}.` Only an approved, scanned `PublicReason` is shown. An absent or protected reason gives `The query result could not be validated and was withheld.` | Use the stated reason to investigate the returned data. |
| finalization | terminal completion write failed | no | `The query failed and no result was returned.` | Retry; if it persists, likely a local system issue. |
| provenance_sink | no provenance path configured (unfixable) | no | `Query execution is not available in this environment. This is a configuration issue that cannot be resolved by changing or retrying the query.` | Stop; operator/config fix required. Do not retry. |
| provenance_sink | transient I/O (preflight or append) | no | `The query failed and no result was returned.` | Retry later. |
| remote_execution | real API error that passes disclosure checks | (per HTTP status) | *(passthrough — verbatim API error, with its normal code and HTTP status)* | Follow the API error. |
| remote_execution | fallback, transient (408/429/5xx) | (loop) | `The query failed and no result was returned.` | Retry shortly. |
| remote_execution | fallback, permanent (4xx) | no | `The query could not be run as written.` | Review/correct the query. |

Note: `The query failed and no result was returned.` and `The query could not be
run as written.` are intentionally reused across several rows so restricted
output has fewer distinguishable strings (harder to fingerprint replay).

## Open decisions (need sign-off)

1. **Preparation hints — DECIDED.** Mine the typed `ReplayError` fields
   (`Code` + `Construct` + `Remedy` + `PublicMessage`), never the detailed
   `Message` (it can contain "replay").
   Author a hint for user-fault codes (unsupported element / timeframe / shift),
   pass through plain parse errors, keep internal-mismatch codes generic. Compose
   then guard-word-scan with generic fallback.
1b. **Surface specific validation reasons — DECIDED (yes).** Approve public
   reasons at their error-creation sites. Keep internal contract, source-identity,
   and provenance failures generic. Scan the approved `PublicReason` before
   displaying it. An absent or protected reason uses the generic fallback.
2. **Split `provenance_sink` config vs transient — DECIDED (yes).** Config-missing
   (`no provenance path` / nil factory) → distinct "cannot be resolved" env
   message; all other sink I/O → generic execution failure.
3. **Expose the 5s cadence minimum — DECIDED (yes).** Keep the numbers.
4. **Command-guard recording failures — DECIDED (2026-09-15).** Use the same
   `this command is not available in this context` message whether recording
   succeeds or fails. Use the same `command_unavailable` agent code and the same
   suggestions for switching context. Keep the recording attempt and both
   internal causes. Capture the detailed rejection before selecting restricted
   output so the private log retains it.

All decisions are now settled; see the implementation plan below.

---

## Implementation plan

### Design summary

`restricted` disclosure changes from "one fixed generic string per category" to
"describe the failure well enough to act, never reveal the replay machinery."
Every surfaced string is either **authored** by us or **content-scanned** before
passthrough. Generated-content and mapping checks cover known disclosures. Full disclosure is
unchanged. Disclosure modes stay `full`/`restricted` (no config/enum change).

Two internal concepts drive the richer restricted messages:
- The compiler's typed `execreplay.ReplayError` (`Code`/`Construct`/`Remedy`/`PublicMessage`)
  lets us author a safe query hint without touching its replay-worded `Message`.
- A shared content scanner (generalized from today's `restrictedRemoteTextMayPass`)
  decides whether a candidate string exposes replay internals.

### Change set

#### 1. `pkg/exec/replay_execution.go` (core)

**Message constants (`:154-165`)** — retarget to the finalized strings:
- `restrictedNoDataMessage` → `No data is available for the requested timeframe.`
- `restrictedTemporaryNoDataMessage` → `The requested timeframe is not available yet.`
- `restrictedReadinessMessage` → `This environment is not currently able to serve queries. This is a setup issue that cannot be resolved by changing or retrying the query.`
- `restrictedQueryInvalidMessage` (renamed from `restrictedPreparationMessage`) → `The query could not be run as written.`
- `restrictedValidationMessage` → `The query result could not be validated and was withheld.` (fallback only)
- `restrictedGenericExecutionMessage` (new; replaces finalize/sink/remote generics) → `The query failed and no result was returned.`
- `restrictedSinkConfigMessage` (new) → `Query execution is not available in this environment. This is a configuration issue that cannot be resolved by changing or retrying the query.`
- Remove `restrictedFinalizationMessage`, `restrictedPreflightSinkMessage`, `restrictedPostExecutionSinkMessage`, `restrictedRemoteExecutionMessage` (folded into the generics above).
- `fullTemporaryNonOverlapMessage` unchanged.

**`restrictedMessageForInfo` (`:298-324`)** — new routing:
- `non_overlap`: retryable → temporary; else no-data. (unchanged shape)
- `readiness`: readiness message.
- `preparation`: delegate to `restrictedPreparationMessage(detail, info)` (below).
- `result_validation`: delegate to `restrictedValidationMessage(detail, info)` (below).
- `finalize`: `restrictedGenericExecutionMessage`.
- `sink`: `errors.Is(detail, ErrReplayProvenancePathMissing)` OR `ErrReplayProvenanceSinkUnavailable` → `restrictedSinkConfigMessage`; else `restrictedGenericExecutionMessage`. (`postExecution` no longer needed for selection — drop it from the signature.)
- `remote`: if `restrictedRemoteTextMayPass(detail, info)` → `detail.Error()`; else if `replayRemoteFailurePermanent(detail)` → `restrictedQueryInvalidMessage`; else `restrictedGenericExecutionMessage`.
- default: `restrictedQueryInvalidMessage`.

**New `restrictedPreparationMessage(detail, info)`**:
1. `errors.As(detail, *ReplayCadenceError)` → `fmt.Sprintf("The query is being run too frequently; the minimum time between runs is %s (requested %s).", min, requested)`.
2. `errors.As(detail, *execreplay.ReplayError)` → `preparationHintFromReplayError`; scan it (metric-terms allowed); return hint if clean, else `restrictedQueryInvalidMessage`.
3. plain parse error about the user's own query: require `info.EffectiveQuery == ""` and a real `QueryError`; check `restrictedOriginalParseTextMayPass` before returning `detail.Error()`. Ordinary metric words, the user's snapshot table, and quoted DQL fragments are allowed without a mapping. Hard guard words and generated content still block. Any parse error after rewriting stays generic.
4. else `restrictedQueryInvalidMessage`.

**New `preparationHintFromReplayError(re)`** — switch `re.Code`:
- `ErrorUnsupportedForm`/`ErrorUnsupportedSource` → `"The query uses an unsupported element: " + re.Construct + "." (+ " " + re.Remedy)`
- `ErrorTimeframe` → approved `PublicMessage`, or `""` for an unclassified/internal failure. `queryTimeframeError` authors guidance for rejected user time expressions. The unsupported-expression, non-UTC alignment, and one-nanosecond cases have dedicated public messages.
- `ErrorShift` → `"The query uses an unsupported time shift. Remove the time shift."`
- `ErrorAudit`/`ErrorASTContract`/`ErrorResultContract`/`ErrorCurrentState`/default → `""` (→ generic).

**`restrictedValidationMessage(detail, info)`**: use `errors.As` to recognize a
`ResultContractError`, including wrapped errors. Select its optional
`PublicReason`, never its detailed `Reason` or replay-prefixed `Error()` text.
Plain metadata-inspection errors still use their error text. A nonempty reason
that passes the scanner gives `"The query result failed a consistency check: " +
reason + "."`. Otherwise use `restrictedValidationFallbackMessage`. Ordinary
metric terms are allowed; invalid results remain withheld.

**Scanner rules**: `containsGeneratedReplayText` protects generated timestamps
and rewritten DQL for every remote error. `containsDavisMappingText` adds
reconstruction fragments and vocabulary only when execution metadata records
an actual Davis mapping. Ordinary API diagnostics may therefore contain words
such as `filter`, `sort`, and `timestamp`. The remote check also scans the error
wrapper and strips original user DQL so that it does not mistake the user's own
text for generated content.

Authored guidance and validation reasons use `replayTextExposesInternals`.
They retain the hard guard words, generated timestamp checks, and reconstruction
checks. `allowMetricTerms=true` permits `interval` and ordinary natural-bucket
wording in these paths.

Original-query parse diagnostics use `restrictedOriginalParseTextMayPass`.
It scans the whole diagnostic for hard guard words and generated content.
Reconstruction vocabulary blocks only when metadata records a Davis mapping.
This exception does not change the checks on authored guidance.

#### 2. Typed errors for detection (keep full-mode `Error()` text identical)

- `ReplayCadenceError{Requested, Minimum time.Duration}` at the cadence-validation site (`ValidateReplayCadence`, `pkg/exec/replay_executor.go:260` / preparer `ValidateCadence`). Its `Error()` returns today's exact string so full disclosure and existing tests are unaffected.
- Sentinels `ErrReplayProvenancePathMissing`, `ErrReplayProvenanceSinkUnavailable` wrapped at `pkg/exec/replay_preparer.go:359` and `:363`; keep the same wrapped message text.
- `ReplayError.PublicMessage` and `ResultContractError.PublicReason` carry
  optional approved text. Both are excluded from JSON and YAML serialization.
  Existing `Error()` text and private provenance retain their detailed reasons.

#### 3. Tests (`pkg/exec/replay_executor_integration_test.go`)

- Retarget existing restricted-message expectations to the new strings.
- Add: each preparation `ReplayError` code (unsupported form/source → element+construct; timeframe; shift; audit → generic); cadence typed error → numbers present, no guard word; plain original-query parse error → passthrough; effective-query parse error → masked; validation concrete reason surfaced (incl. an `interval`/`natural bucket` case); sink config vs transient; remote permanent (4xx) vs transient fallback.
- **Leak-guard test**: table of every restricted constant + a corpus of representative details asserted to contain none of the hard tells and not to equal the raw replay-worded `Message`.
- **Message inventory**: `replay_message_inventory_test.go` enumerates production
  compiler and validator error-construction sites from Go source. Its golden
  records guidance or intentional generic output for real typed errors and
  wrappers. It includes formatted values and protected samples. Adding a site
  or changing a remedy, reason, or public outcome requires reviewing the golden.
- **Executor regressions**: real validator failures still withhold results and
  retain private details. The recorded calendar-duration parser fixture passes
  through before rewriting and stays generic after rewriting. The three
  timeframe cases retain their preparation-error and retry behavior. These
  tests remain separate from the authored-message inventory.
- **Parser disclosure regressions**: a direct snapshot query keeps a parser
  diagnostic naming its table. Original user fragments pass through, while
  rewritten-query errors, generated content, and authored reconstruction
  guidance remain protected.
- **Guard recording regressions**: missing-path, preflight, and append failures
  produce the same agent fields as successful recording. Tests cover commands,
  plugins, wrapped errors, private causes, and detailed private log entries.

#### 4. Golden files

- Regenerate restricted error goldens under `pkg/output/testdata/golden/errors/` via `make test-update-golden`; diff-review that no replay term leaked and full-mode goldens are unchanged.

#### 5. Docs

- `cmd/replay.go` restricted-disclosure help text (`:52, 71-73, 126-133`): update the one-line description of restricted output (now actionable, still replay-silent).
- This working doc stays as the message/decision reference.

### Non-goals

- No new disclosure mode; `restricted` semantics change in place.
- No change to metadata suppression (`replay_output.go` / `agent.go` stay nil in restricted).
- Suggested actions (the "Suggested action (DEFERRED)" column) are **not** wired in this change — messages only.

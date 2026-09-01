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

- **Block content leaks:** never emit the rewritten query, a virtual timestamp,
  a reconstruction source/table, the context name, or the hard-tell guard words
  `replay / virtual / session / clock / effective`.
- **Metric-term exception:** `interval` and `natural bucket` are ordinary DQL
  metric vocabulary, not replay tells. They are permitted in authored
  preparation hints (an unsupported metric `interval` element) and in
  result-validation reasons; the scanner drops them from the guard set only in
  those two authored paths (`allowMetricTerms`). Everywhere else `interval`
  stays blocked.
- **Behavioral leaks are acceptable:** a message may describe an ordinary-looking
  failure that happens to only occur in replay (a 5s minimum, a point-count
  re-check). An LLM reads these as normal query problems.

Every committed message string is authored (not passed through), so leak-safety
is guaranteed by construction. A regression test asserts each string contains
none of the guard words.

## Messages and (deferred) suggested actions

| Category | Sub-case | Retryable | Error message (restricted, committed) | Suggested action (DEFERRED) |
|---|---|---|---|---|
| readiness | no active session / completed / config drift | no | `This environment is not currently able to serve queries. This is a setup issue that cannot be resolved by changing or retrying the query.` | Stop; operator/setup intervention required. Do not retry. |
| non_overlap | data not visible yet (realtime, waiting) | yes | `The requested timeframe is not available yet.` | None needed — auto-retried; safe to wait. |
| non_overlap | timeframe outside available data | no | `No data is available for the requested timeframe.` | Try a different (e.g. earlier) timeframe. |
| preparation | cadence too fast | no | `The query is being run too frequently; the minimum time between runs is {min} (requested {actual}).` | Reduce polling frequency to ≥ {min}. |
| preparation | unsupported function/command/parameter (`unsupported_form`/`unsupported_source`) | no | `The query uses an unsupported element: {construct}. {remedy}` | Correct/remove the named element. |
| preparation | bad/dynamic timeframe (`timeframe`) | no | `The query's timeframe could not be interpreted. {remedy}` | Use an absolute start/end timeframe. |
| preparation | unsupported time shift (`shift`) | no | `The query uses an unsupported time shift. {remedy}` | Remove/adjust the shift. |
| preparation | plain DQL syntax error (user's own query, real `QueryError`) | no | *(passthrough — verbatim parse error, scanned)* | Fix the reported syntax error. |
| preparation | internal audit / ast_contract / result_contract / Davis mismatch | no | `The query could not be run as written.` | Not query-fixable; report/retry. |
| result_validation | returned data failed contract checks | no | `The query result failed a consistency check: {reason}.` (concrete reason surfaced; ordinary metric vocabulary such as "interval"/"natural bucket" is allowed here, only hard tells fall back to generic `The query result could not be validated and was withheld.`) | Try a simpler query (fewer fields / single metric). |
| finalization | terminal completion write failed | no | `The query failed and no result was returned.` | Retry; if it persists, likely a local system issue. |
| provenance_sink | no provenance path configured (unfixable) | no | `Query execution is not available in this environment. This is a configuration issue that cannot be resolved by changing or retrying the query.` | Stop; operator/config fix required. Do not retry. |
| provenance_sink | transient I/O (preflight or append) | no | `The query failed and no result was returned.` | Retry later. |
| remote_execution | real API error, proven replay-free | (per HTTP status) | *(passthrough — verbatim API error, unchanged)* | Follow the API error. |
| remote_execution | fallback, transient (408/429/5xx) | (loop) | `The query failed and no result was returned.` | Retry shortly. |
| remote_execution | fallback, permanent (4xx) | no | `The query could not be run as written.` | Review/correct the query. |

Note: `The query failed and no result was returned.` and `The query could not be
run as written.` are intentionally reused across several rows so restricted
output has fewer distinguishable strings (harder to fingerprint replay).

## Open decisions (need sign-off)

1. **Preparation hints — DECIDED.** Mine the typed `ReplayError` fields
   (`Code` + `Construct` + `Remedy`), never the `Message` (it contains "replay").
   Author a hint for user-fault codes (unsupported element / timeframe / shift),
   pass through plain parse errors, keep internal-mismatch codes generic. Compose
   then guard-word-scan with generic fallback.
1b. **Surface specific validation reasons — DECIDED (yes).** Emit the concrete
   reason; rephrase the `interval`/`natural bucket` variants; generic fallback on
   any guard-word hit.
2. **Split `provenance_sink` config vs transient — DECIDED (yes).** Config-missing
   (`no provenance path` / nil factory) → distinct "cannot be resolved" env
   message; all other sink I/O → generic execution failure.
3. **Expose the 5s cadence minimum — DECIDED (yes).** Keep the numbers.

All decisions are now settled; see the implementation plan below.

---

## Implementation plan

### Design summary

`restricted` disclosure changes from "one fixed generic string per category" to
"describe the failure well enough to act, never reveal the replay machinery."
Every surfaced string is either **authored** by us or **content-scanned** before
passthrough, so leak-safety is guaranteed by construction. Full disclosure is
unchanged. Disclosure modes stay `full`/`restricted` (no config/enum change).

Two internal concepts drive the richer restricted messages:
- The compiler's typed `execreplay.ReplayError` (`Code`/`Construct`/`Remedy`)
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
3. plain parse error about the user's own query: `restrictedRemoteTextMayPass(detail, info)` → `detail.Error()` (the effective-query parse error fails the timestamp check here and correctly falls through).
4. else `restrictedQueryInvalidMessage`.

**New `preparationHintFromReplayError(re)`** — switch `re.Code`:
- `ErrorUnsupportedForm`/`ErrorUnsupportedSource` → `"The query uses an unsupported element: " + re.Construct + "." (+ " " + re.Remedy)`
- `ErrorTimeframe` → `"The query's timeframe could not be interpreted." (+ Remedy)`
- `ErrorShift` → `"The query uses an unsupported time shift." (+ Remedy)`
- `ErrorAudit`/`ErrorASTContract`/`ErrorResultContract`/`ErrorCurrentState`/default → `""` (→ generic).

**New `restrictedValidationMessage(detail, info)`**: if `detail != nil` and the
text does not expose internals (metric-terms allowed) → `"The query result failed a consistency check: " + reason` ; else `restrictedValidationMessage` fallback.

**Scanner refactor**: extract `replayTextExposesInternals(text, info, allowMetricTerms bool) bool` from the guard-word / timestamp-instant / Davis-mapping / verbatim-original-query logic now inside `restrictedRemoteTextMayPass` + `containsRestrictedGeneratedWord` + `containsDavisMappingText`. `restrictedRemoteTextMayPass` becomes: strip the verbatim `QueryError`/`APIError` body, then `!replayTextExposesInternals(rest, info, /*strict*/ false-allow=false)`. `allowMetricTerms=true` drops `interval` (and treats "natural bucket" as ordinary) from the guard set; the hard tells (`replay/virtual/session/clock/effective`, virtual timestamps, rewritten/effective query, Davis reconstruction tokens) always block.

#### 2. Typed errors for detection (keep full-mode `Error()` text identical)

- `ReplayCadenceError{Requested, Minimum time.Duration}` at the cadence-validation site (`ValidateReplayCadence`, `pkg/exec/replay_executor.go:260` / preparer `ValidateCadence`). Its `Error()` returns today's exact string so full disclosure and existing tests are unaffected.
- Sentinels `ErrReplayProvenancePathMissing`, `ErrReplayProvenanceSinkUnavailable` wrapped at `pkg/exec/replay_preparer.go:359` and `:363`; keep the same wrapped message text.

#### 3. Tests (`pkg/exec/replay_executor_integration_test.go`)

- Retarget existing restricted-message expectations to the new strings.
- Add: each preparation `ReplayError` code (unsupported form/source → element+construct; timeframe; shift; audit → generic); cadence typed error → numbers present, no guard word; plain original-query parse error → passthrough; effective-query parse error → masked; validation concrete reason surfaced (incl. an `interval`/`natural bucket` case); sink config vs transient; remote permanent (4xx) vs transient fallback.
- **Leak-guard test**: table of every restricted constant + a corpus of representative details asserted to contain none of the hard tells and not to equal the raw replay-worded `Message`.

#### 4. Golden files

- Regenerate restricted error goldens under `pkg/output/testdata/golden/errors/` via `make test-update-golden`; diff-review that no replay term leaked and full-mode goldens are unchanged.

#### 5. Docs

- `cmd/replay.go` restricted-disclosure help text (`:52, 71-73, 126-133`): update the one-line description of restricted output (now actionable, still replay-silent).
- This working doc stays as the message/decision reference.

### Non-goals

- No new disclosure mode; `restricted` semantics change in place.
- No change to metadata suppression (`replay_output.go` / `agent.go` stay nil in restricted).
- Suggested actions (the "Suggested action (DEFERRED)" column) are **not** wired in this change — messages only.


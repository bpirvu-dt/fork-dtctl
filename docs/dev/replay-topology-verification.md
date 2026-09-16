# Replay topology live verification

The v11 section 4.5 traversal check passed on 2026-09-15 in both full and
restricted disclosure. It used a post-fix recording from the previous day,
over 42 hours old at execution, and no request default-timeframe flags.

## Control and result

The same source and destination service identities were present in both
one-minute windows. Direct node queries confirmed that fact before the
traversal ran. Direct `smartscapeEdges "calls"` queries found the selected
edge only in the first window.

| Check | 03:30–03:31 UTC | 04:30–04:31 UTC |
|---|---:|---:|
| Both selected endpoint identities present | Yes | Yes |
| Selected direct calls edge present | Yes | No |
| Traversal reaches selected destination | Yes | No |
| Traversal result count | 6 | 3 |
| Every traversal result is in the window's bare SERVICE node set | Yes | Yes |
| Full and restricted result files match byte-for-byte | Yes | Yes |

Both windows were on 2026-09-14. The recording's fault was
`service_dns_resolution_failure_social_network`. Neither selected endpoint
disappeared. Other services in the recording did disappear, but they were
not the endpoints used to establish edge-window isolation.

The check called "belongs to the selected leg" tested membership in each
window's bare `SERVICE` node set. Those sets contained 12 nodes in the first
window and 10 in the second. Every traversal result belonged to the
corresponding set. This did not independently prove membership in the
selected leg or equality with the application's full service set.
The leg-identification capture contains a one-service sample. It identifies
that service's leg; it is not a membership proof for all returned services.

Replay used a manual clock, with virtual now at each window's end. The query
selected one source service from bare `smartscapeNodes "SERVICE"`, followed
by `traverse "calls", "SERVICE"`. The target-type parameter is required by
the service parser. The compiler inserted the source's 60-second bounds.
The traversal text stayed unchanged.

The first run passed without the request-timeframe fallback. No fallback
was added. This result therefore does not claim to test that conditional
fallback or its differing-feeder-window rejection.

## Disclosure and audit evidence

Full explain output recorded source class `topology`, boundary policy
`window_only`, and the traversal's feeder path and effective window.
Restricted provenance recorded identical effective DQL at each virtual now.
Restricted ordinary stderr was empty. The two disclosures returned identical
JSON data at each window.

The first restricted start correctly rejected a non-private evidence
folder. After its permissions were changed to 0700, both restricted checks
passed. Existing user replay sessions were untouched; the test used a
separate config and state directory and stopped both sessions afterwards.

## Capture location

Private raw observations, query text, command transcript, lifecycle output,
explain records, provenance, binary hash, `README.md`, and `SHA256SUMS` are
stored outside Git in the workspace's sibling capture directory:

```text
~/devel/dt/captures/replay-v11-traverse-2026-09-15/
```

The primary machine-checked summaries are `08-verification.json` and
`09-effective-dql-equality.json`. The capture contains real topology IDs
and is deliberately excluded from the repository. Compiler fixtures use
only synthetic data under `sdk/api/query/testdata/topology/`.

## Acceptance scenario map

These checks separate fixture-backed automated verification from the live
execution above. The test names below are in the implementation tree.

| V11 scenario | Verification |
|---|---|
| 1–2: bare nodes and calls use the previous minute | `TestTopologyCapturedCompilerAndAudit`; `TestDQLExecutorTopologyDisclosuresUseIdenticalDQLAndRecords` |
| 3: a wider explicit source window is preserved | `TestTopologyCapturedCompilerAndAudit`; `TestTopologyTimeframePrecedenceAndWidth` |
| 4–5: sub-minute rejection, generic restricted output and private reason/window | `TestDQLExecutorTopologyWidthFailuresAreHardAndRecorded` |
| 6: traversal changes with the edge while both endpoints stay visible | Captured live check above, in both disclosures |
| 7: feeder-less traversal is rejected | `TestTopologyRejectedCapturedShapes`; `TestDQLExecutorTopologyAuthorizationRejectsBeforeExecute` |
| 8: retention policy and limitation | User documentation; `TestDQLExecutorTopologyRetentionIsNotVerifiedForPureAndMixedQueries` checks the emitted result |
| 9–10: minimum startup history, omitted start, invalid restart, old sessions | `TestResolveReplayConfigMinimumStartupHistory`; `TestReplayCLIStartupHistoryAndInvalidRestart`; `TestReplayCLILegacyStartupHistoryKeepsStatusAndStopUsable`; `TestDQLExecutorLegacyReplayStartupHistoryFailsReadiness` |
| 11: timeframe precedence | `TestDQLExecutorTopologyTimeframePrecedence`; `TestTopologyInvalidTimeframesDoNotUseFallback` |
| 12: verified edge selectors only | `TestTopologyRejectedCapturedShapes`; `TestDQLExecutorTopologyAuthorizationRejectsBeforeExecute` |
| 13: feeder/window explain and provenance | `TestReplayTopologyExplainShowsStructuralFeederAndWindow`; `TestDQLExecutorTopologyDisclosuresUseIdenticalDQLAndRecords`; captured live explain/provenance |
| 14: realtime wait/live retries a provably widening sub-minute window and executes at 60 seconds | `TestDQLExecutorTopologyTemporaryWidthRecomputesAndThenExecutes`; `TestDQLExecutorTopologyWidthFailuresAreHardAndRecorded` covers one-shot/manual and permanent rejection; `TestDQLExecutorTopologyTerminalWidthNeverRetries` covers the replay endpoint |

The parser corpus also passes the complete replay AST adapter and SDK
round-trip checks. Audit mutation tests cover changed bounds, command keys,
token placement and feeder bindings. Executor failure tests cover parse,
audit, remote execution and provenance failures.

## Local automated checks

The following checks passed on macOS during implementation:

| Check | Scope |
|---|---|
| CLI tests | `go test . ./cmd/... ./pkg/... ./skills/... ./test/...` |
| SDK tests | `go test ./...` from `sdk/` |
| CLI race checks | `go test -race ./cmd ./pkg/exec ./pkg/exec/replay ./pkg/output` packages; `cmd` also passed a separate sequential rerun |
| SDK race checks | `go test -race ./session ./api/query` from `sdk/` |
| Static checks | CLI `go vet -tags integration`, SDK `go vet`, and both modules' `golangci-lint` |
| Formatting and dependencies | `goimports`, `git diff --check`, and `go mod tidy -diff` in a temporary copy of project files |
| Cross-compilation | Linux/amd64 and Windows/amd64 test binaries for `cmd`, `pkg/exec`, `pkg/exec/replay`, `sdk/session`, and `sdk/api/query` |

The explicit CLI package list excludes an unrelated local SREGym checkout
that an unrestricted `./...` also discovers. No dependency changes were
needed. Earlier overlapping command-suite runs failed; sequential normal
and race reruns passed without a code change. Those earlier failures were
not reproduced.

An independent review checked selector validation, structural feeders,
timeframe precedence, width failures, audit integrity, disclosure routing,
and retention behavior. It found no concrete correctness or policy bypass.
This local review does not replace the normal pull-request review.

## Remaining delivery checks

The required live traversal check is complete. Local automated suites ran
on macOS. Linux and Windows were compile-checked only; their CI test runs
remain outstanding. Review, merge and the normal delivery cycle also remain
outstanding. The full v11 definition of done is therefore not yet complete.

## Scope

This is one discriminating pair on one post-fix fault recording. It proves
that traversal followed the feeder's edge window in this required case.
It does not establish immutable field values, universal relationship-history
fidelity, or complete topology retention. Those remain the documented,
accepted limitations of replay topology.

# The dtctl Config Contract

**Status:** v1, normative since 2026-07-12
**Audience:** dtctl and any `dtctl-*` plugin that reads the shared
configuration. This is the state contract: everything a second binary may
rely on, and nothing more. Golden fixtures live in
`sdk/session/testdata/contract/`; `sdk/session/contract_test.go` enforces this
document. A change that breaks those tests is a contract change and must
update this spec in the same PR.

## File locations

| Artifact | Path |
|---|---|
| Global config | `$XDG_CONFIG_HOME/dtctl/config` (typically `~/.config/dtctl/config`) |
| Project-local config | `.dtctl.yaml`, discovered upward from the working directory; used **exclusively** (never merged with the global file) |
| Explicit config | `--config <path>` flag, wins over discovery |
| OAuth file store | `$XDG_DATA_HOME/dtctl/oauth-tokens/<sanitized-name>.json`, mode 0600 (dir 0700) |
| Token-refresh lock | `$TMPDIR/dtctl-token-refresh-<sha256[:8] of env:tokenRef>.lock` |
| Replay state | `$XDG_STATE_HOME/dtctl/replay/<sha256>.state.json`, Unix mode 0600 (dir 0700) |
| Restricted replay provenance | `$XDG_STATE_HOME/dtctl/replay/<sha256>.provenance.jsonl` by default, Unix mode 0600 |

Security note: code-execution keys (aliases, apply hooks) in an
auto-discovered `.dtctl.yaml` are loaded for round-tripping but **never
honored** — see `Config.IsLocal()`.

## Schema (v1)

YAML document. Top-level keys: `apiVersion`, `kind`, `current-context`,
`contexts` (list of `{name, context}`), `tokens` (list of `{name, token}`),
`preferences`, `aliases`, `spill`. Per-context keys: `environment`,
`token-ref`, `safety-level` (`readonly` | `readwrite-mine` | `readwrite-all` |
`dangerously-unrestricted`; empty means `readwrite-all`), `description`,
`hooks`, `spill`, `profile`, `locale`, `timezone`, and `replay`. The replay
object has `data_start`, `data_end`, `virtual_start`, `clock_mode`,
`disclosure`, and `provenance_path`. The Go structs in
`sdk/session/config.go` and `sdk/session/replay_config.go` are the schema's
source of truth. The contract fixtures exercise the shared core, and
`replay_config_test.go` covers replay YAML round-tripping and validation.

Semantics both binaries must share: `safety-level` (a `readonly` context means
the same thing everywhere) and token resolution order (see below).

### Version policy

- `apiVersion` spellings accepted as schema v1: **empty** (pre-enforcement
  configs), **`v1`**, and **`dtctl.io/v1`** (written by `dtctl config init`).
- An unrecognized `apiVersion` is a **hard load error** naming the version —
  never a silent misread. This is the version-skew answer: "consumer N supports
  config schema ≤ M" is testable.
- Within v1 the schema evolves **additively only**. Renaming or redefining an
  existing key requires bumping the version.

### Tolerant parsing and round-trip preservation

- Readers ignore unknown keys (yaml.v3 default — do not enable strict mode).
- Writers must not destroy unknown keys: `Config.SaveTo` grafts keys unknown
  to the running build from the file being overwritten back into the saved
  document (top level, per-context and per-token matched by `name`, and
  nested structs — see `sdk/session/preserve.go`). Known keys are owned by the
  writer: deleted contexts and cleared `omitempty` fields stay deleted.
- Comments and key order are **not** preserved; only data survives.

### Environment variable expansion

`$VAR` / `${VAR}` in the file expand from the process environment at load.
Shell positional/special parameters (`$1`, `${10}`, `$@`, …) are preserved
verbatim so hook commands survive. Unset variables expand to the empty
string. Management commands that rewrite the file must load with
`LoadWithoutExpansion` so templates round-trip unexpanded.

## Credential store

- **OS keyring service name: `dtctl`** — shared by every consumer; changing
  it strands all stored credentials.
- Key formats: plain API tokens under their token-ref name; OAuth token sets
  (JSON) under `oauth:<env>:<tokenRef>` with `<env>` ∈ `prod` | `dev` |
  `hard`, legacy entries under `oauth:<tokenRef>`.
- Token resolution order (in `Config.GetToken`): keyring OAuth entry →
  keyring plain token → OAuth file store (when the keyring is unavailable or
  `DTCTL_TOKEN_STORAGE=file`) → inline `token` value in the config file.
- `DTCTL_DISABLE_KEYRING` (any non-empty value) disables the keyring;
  `DTCTL_TOKEN_STORAGE=file` forces the file store.
- **macOS keychain UX**: keychain access is granted per binary, so each
  consumer (dtctl and every plugin) triggers its own one-time
  keychain-access prompt on first credential read. Expected behavior —
  document it, don't "fix" it.

## Write rules

1. **dtctl owns all config-file writes** — context CRUD, login flows,
   safety-level assignment. Plugins treat the config file as **read-only**.
2. **The token store is the one shared write surface.** OAuth refresh tokens
   rotate on use, so any long-running consumer must persist refreshed token
   sets — and must do so through the cross-process refresh lock
   (`sdk/session`, `TokenManager`), never with an unlocked read-modify-write.
   Concurrent unlocked refreshes double-spend the rotating refresh token and
   strand one side's credentials (`invalid_grant`).
3. **Context overrides are session-local.** The `--context` flag and the `DTCTL_CONTEXT` env var override the current context in
   memory only. The sole way to persist a switch is `dtctl ctx <name>`
   (or `dtctl config use-context`).

## Replay configuration and state

A replay context stores stable input only:

```yaml
contexts:
  - name: historical-window
    context:
      environment: https://example.apps.dynatrace.com
      token-ref: readonly-reader
      safety-level: readonly
      profile: replay
      replay:
        data_start: "2026-06-14T08:00:00Z"
        data_end: "2026-06-14T12:00:00Z"
        virtual_start: "2026-06-14T10:00:00Z"
        clock_mode: manual
        disclosure: restricted
```

The example is for automation. Automated examples use explicit `manual` clock
mode and `restricted` disclosure. `virtual_start` defaults to `data_start`.
`clock_mode` defaults to `realtime`. `disclosure` defaults to `full`.
`data_start`, `data_end`, and `virtual_start` are absolute RFC 3339 timestamps.
They are normalized to UTC in runtime state. `data_start` must be earlier than
`data_end`. `virtual_start` may equal either replay interval boundary.

`replay start` may override the three timestamp fields and `clock_mode`. A flag
wins over a context field. Disclosure and provenance path have no start-command
override. A flags-only session therefore uses full disclosure.

Session IDs, host timestamps, clock anchors, resolved value sources, completion
state, and stop state belong to the private replay state file. They are not
written to the context. Filenames use a SHA-256 digest of the canonical config
source, context name, and normalized environment URL. They do not contain a
context name, URL, or token reference. State is versioned and contains no token
or telemetry.

Normal queries and `replay status` read state without a state lock and without
write access. `replay start`, `advance`, `stop`, restart, and guarded terminal
completion use the mandatory cross-process writer lock and atomic replacement.
The lock implementation is platform-specific on Unix and Windows.

Restricted disclosure requires a private JSON Lines provenance file. An
omitted path resolves below the replay state directory. An override must be an
absolute safe path. The implementation checks the parent, ownership and private
modes where supported, and refuses symlinks. A separate cross-process lock
serializes complete appended records. Each record is flushed before the lock is
released. Full disclosure creates no provenance file and has no provenance
dependency.

On Unix, the replay directory uses mode `0700`; state, provenance, and lock
files use mode `0600`. On Windows, dtctl applies and validates a private DACL
for the current user, local administrators, and `SYSTEM`.

A usable state snapshot owns its stored disclosure and provenance route even
after the context drifts. This prevents a context edit from widening a running
restricted session. Context drift blocks query execution and requires restart.

A replay block is also a configured-but-inactive guard signal. If the block is
present but no active session exists, DQL fails closed. A flags-only session in
a context without a replay block loses that configured signal after stop. This
is why automation must persist the complete block.

The reserved profile name `replay` cannot appear under user-defined
`profiles:`. Config validation rejects that collision and asks the user to
rename the custom profile.

## Environment variable overrides

| Variable | Meaning |
|---|---|
| `DTCTL_CONTEXT` | Session-local current-context override; flag `--context` wins over it. Exported to plugins. |
| `DTCTL_OUTPUT` | Default output format when `-o/--output` is not given (dtctl only). |
| `DTCTL_DISABLE_KEYRING` | Disable the OS keyring (any non-empty value). |
| `DTCTL_TOKEN_STORAGE` | `file` forces the file-based OAuth store. |

## Golden fixtures

| Fixture | Asserts |
|---|---|
| `v1-full.yaml` | Shared core fields parse; unknown fields at all levels are tolerated and survive a load-modify-save cycle |
| `v1-minimal.yaml` | Minimal config loads; `apiVersion` is optional |
| `future-version.yaml` | Unsupported schema version fails loudly |

Replay fields have focused coverage in `replay_config_test.go` because their
resolved defaults, path safety, and UTC normalization are runtime contracts in
addition to YAML schema fields.

The fixtures live in the sdk module (`sdk/session/testdata/contract/`), so
after the repo split both binaries keep testing against the same versioned
artifacts — the fixtures are the compatibility test between independently
released binaries.

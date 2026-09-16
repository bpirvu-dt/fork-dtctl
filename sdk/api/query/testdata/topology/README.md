# Smartscape replay parser evidence

Captured from live `query:parse` on 2026-09-15 through an existing readonly
context. Queries use synthetic timestamps, selectors and field values.
No telemetry was executed to produce these fixtures. Context names,
endpoints, token references and request/correlation IDs are excluded or
redacted before writing JSON.

The corpus has 40 original responses and 25 validation responses. Two
originals are server errors: `traverse-own-from` and
`traverse-single-selector`. The remaining 63 responses contain server ASTs.
Original fixtures were captured before their compiler tests were written.
Validation fixtures parse the paired `effective.dql` text.

## Clock used for validation fixtures

Unless listed below, replay has `data_start=2026-08-01T10:00:00Z`,
`virtual_start=2026-08-01T10:01:00Z`, `virtual_now=2026-08-01T12:00:00Z`,
and `data_end=2026-08-01T14:00:00Z`. Bare topology resolves to 11:59–12:00.
Explicit source windows resolve to 11:00–12:00.

- `nodes-defaults` uses request defaults of 10:30–11:00 on that day.
- `old-nodes-control` preserves an August rejection-control query. Its
  replay interval starts at `2026-08-10T10:50:02.718012207Z`, virtual now is
  `2026-08-10T10:55:02.718012207Z`, and data end is
  `2026-08-10T11:05:02.718012207Z`.

## Allowlist findings and rationale

| Construct | Evidence | Rationale |
|---|---|---|
| `smartscapeNodes` | `nodes-*`, `traverse-window` | Topology source whose bounds can be inserted and audited. Node patterns/lists do not select another source. |
| `smartscapeEdges` | `edges-*` | Same window contract. Only exact `calls` and `runs_on` pass the measured-edge-type policy. |
| `traverse` | `traverse-window`, `traverse-limit`, `traverse-filter`, `traverse-fields-add`, `traverse-chain`, `traverse-nested` | Requires a structural topology feeder in the same execution block. Chained traversal keeps that feeder. Live execution evidence is a separate requirement. |
| Traversal options | `traverse-params`, `traverse-edges-feeder` | `direction`, `fieldsKeep`, `nodeId` describe direction, retained input fields, and the seed-ID input field. They do not supply another temporal source. |
| Chain breakers | `traverse-after-data`, `traverse-after-fetch`, `traverse-after-append`, `traverse-after-join`, `traverse-after-lookup`, `traverse-nested-no-feeder` | Parser-valid rejection controls: no topology feeder, another source, or a block boundary. |
| Edge exclusions | `edges-wildcard`, `edges-contains`, `edges-pattern`, `traverse-wildcard`, `traverse-contains` | Parser-valid rejection controls: selectors outside verified edge types. |
| Short window | `nodes-subminute` | Parser-valid five-second request; replay rejects before execute. |
| Traversal bounds | `traverse-own-from` | Server rejects `from`; replay's key check also fails closed. |

## Exact token and key findings

- Source selector key: `type`, repeated once per selector in a braced list.
- Quoted node selector: `SMARTSCAPE_NODE_PATTERN`; unquoted:
  `SMARTSCAPE_NODE_TYPE`.
- Quoted edge selector: `SMARTSCAPE_EDGE_PATTERN`; unquoted:
  `SMARTSCAPE_EDGE_TYPE`. Both require the same scoped placement and exact
  selector validation.
- Traversal keys: `edgeTypes`, `targetTypes`, `direction`, `fieldsKeep`,
  `nodeId`. Group items have informational keys `edgeType`, `targetType`,
  and `field` respectively.
- Traversal requires edge and target-node selectors. `traverse "calls"`
  alone is rejected by the service.
- An edges feeder needs an explicit seed field because edges lack `id`.
  The fixture uses `nodeId:source_id`.
- The service canonicalizes source bounds before the selector, despite
  selector-first submitted text. Validation captures retain actual token
  positions in that submitted effective text.

## Privacy verification

The sanitizer redacts request/correlation IDs and token-shaped strings.
Committed text was scanned for tenant domains/identifiers, token prefixes,
email addresses, context names, and local account paths. Queries and field
values are synthetic. Raw live topology observations stay outside Git.

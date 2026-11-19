# laserstream-cursor-measure

Independent, self-contained measurement for
[helius-labs/laserstream-sdk#115](https://github.com/helius-labs/laserstream-sdk/issues/115)
("Replay cursor advances from user slot updates, so mid-replay reconnects can
skip undelivered payloads").

A local mock geyser server replays the exact two-window story of the issue:

- connection 1 (replay): payload slots 90 and 91, then one slot tick at live
  slot 122, then the stream ends;
- connection 2: the mock records the `from_slot` of the SDK's reconnect
  request — that request **is** the measurement.

The mock replays its fixed script regardless of the `from_slot` it receives,
so the numbers below say nothing about any real server; they say what the
SDK asks for after the disconnect.

## Measured (helius-laserstream 2.0.0 from crates.io == SDK `main` @ `379a6ba`, laserstream-core 14.0.0, CONFIRMED request, replay on)

| script | reconnect `from_slot` | slots a from_slot-honoring server would never re-deliver |
|---|---|---|
| A: PROCESSED tick @122 via user slot filter, no `filter_by_commitment` (#115 case 1) | **122** | 92–121 (30 slots) |
| B: CONFIRMED tick @122 via user slot filter with `filter_by_commitment: true` — tick status == request commitment (#115 case 2) | **122** | 92–121 (30 slots) |
| C: tick @122 tagged only with the SDK's internal filter id (the face PR #114 fixes) | **122** | 92–121 (30 slots) |
| control: no slot update delivered | *(unset)* | none |

- PR #114 (open, unmerged) closes face C only; A and B still advance the
  cursor past undelivered payloads on current `main`.
- The false-positive guard holds: with no slot update in the stream the
  client does not fabricate a cursor (`from_slot` unset).

## Run

```bash
cargo run
# writes laserstream-115-{variant-a,variant-b,variant-c,control}-<date>.json
```

~15 s after the first build, no network, no API key, no SDK checkout: the
harness depends on `helius-laserstream = "2.0.0"` from crates.io. The
published 2.0.0 carries the same cursor code as `main` `379a6ba` (verified
line-by-line: `tracked_slot = s.slot` on any slot update, payload slots never
advance the cursor), and a run against the local `main` checkout produced
byte-identical numbers for all four variants.

## Artifacts

`out/` holds the JSON produced by the run recorded in the issue thread
(2026-11-19): one file per variant, each with `reconnect_from_slot`,
`expected_resume_slot`, `slots_a_compliant_server_would_never_redeliver`, and
the variant label.

## Limits

- Mock server, not LaserStream: it follows the yellowstone slot-filter
  semantics for who *receives* a tick (user filter without
  `filter_by_commitment` matches every status; with it, only matching
  statuses), but delivery after reconnect is a fixed script.
- Rust SDK only; #115 names the same cursor logic in the Go and JS SDKs.

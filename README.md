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

## The PR #114 two-rule matrix (2026-11-19, compliant-server mode)

The question the thread moved to after
[my 2026-11-19 measurement](https://github.com/helius-labs/laserstream-sdk/issues/115#issuecomment-7016745189):
does the cursor rule PR #114 proposes (`replay_cursor_slot`) reach the faces
the issue reports, when the SERVER is compliant — i.e. reconnect serves from
the requested `from_slot` inclusively? Run it with `HONOR_FROM_SLOT=1`:

| script | main rule | PR #114 rule |
|---|---|---|
| A: PROCESSED tick @122, user filter, no `filter_by_commitment` (#115 case 1) | **loss 92–121** | **loss 92–121 (NOT fixed)** |
| B: CONFIRMED tick @122, user filter with `filter_by_commitment` (#115 case 2) | **loss 92–121** | **loss 92–121 (NOT fixed)** |
| C: tick tagged ONLY with the SDK internal filter (what PR #114 addresses) | loss 92–121 | **fixed**: from=91, one duplicate |
| D: BLOCK FOOTER @122 on a user filter | **loss 92–121** (main's `footer_resume_slot`) | **fixed as a side effect** (BlockFooter omitted from the rule) |
| E: footer-only subscription (no slot updates at all) | **loss 90–121** (cursor rides the live-head footer) | zero loss, but the cursor never advances: `from_slot` unset on every reconnect → permanent duplicates |
| control: payload-only stream | ok (from unset, duplicates 90, 91) | ok (from=91, duplicate 91) |

The headline: PR #114's rule is "any non-internal-filter update advances the
cursor", not "only delivered payloads advance". Its own tests cover exactly
two cases (internal-only slot update ignored; delivered Block advances) — the
user-slot-tick faces A and B, which are the faces #115 is about, still skip
30 slots on a compliant server. A full fix needs the rule to advance ONLY on
account/tx/block-meta payload slots (or their delivered footers), regardless
of which filter matched.

New faces on `main` (not in #115): D — a block footer at live head advances
the cursor past the replay range — and E — a footer-only consumer loses its
whole delivered range when the live-head footer arrives.

Per-cell JSON: `research/laserstream-115-{main,pr114}-{control,variant-a..e}-2026-11-19.json`,
matrix summary: `research/laserstream-115-pr114-two-rule-matrix-2026-11-19.json`.
The PR #114 diff this measured is vendored verbatim at
`tools/pr114-replay-cursor.patch` (branch `bert/replay-cursor-from-delivered-update`,
head `708edd3`).

## Run

```bash
cargo run                      # non-compliant server: makes the skip visible
HONOR_FROM_SLOT=1 cargo run    # compliant server: what the consumer actually receives
# writes laserstream-115-{variant-a,variant-b,variant-c,variant-d-footer,variant-e-footer-only,control}-<date>.json
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

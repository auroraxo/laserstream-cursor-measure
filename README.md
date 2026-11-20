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

## The quiet-subscription age-out measurement (2026-11-20)

The open half of the thread (akshatcoder-hash, 18:20Z): if the cursor advances
ONLY on delivered payload slots, a subscription that goes quiet stops advancing
the cursor — and a replay window that slid past the cursor then answers
`OUT_OF_RANGE`. Measured as a 2×2 over RULE × WINDOW, each with the SDK's
default options (OUT_OF_RANGE is a transient error, silently retried every
5 s) and with `intra_slot_updates = true` (the SDK's switch that makes it
terminal). Scenario: payload@90, payload@91, then ONE tick from the SDK's own
internal slot tracker @122 (the only thing a quiet subscription still
receives), then silence. Aged window = retention floor 130; held window =
floor 0.

| rule | window | from_slot | OUT_OF_RANGE | consumer sees | replay covers |
|---|---|---|---|---|---|
| main 2.0.0 | aged (floor 130) | **122** | yes (2) | **nothing** (0 errors, silent retries) | — |
| main 2.0.0 | aged + intra_slot_updates | 122 | yes | terminal `OutOfRange` error | — |
| PR #114 | aged (floor 130) | **91** | yes (2) | **nothing** (0 errors, silent retries) | — |
| PR #114 | aged + intra_slot_updates | 91 | yes | terminal `OutOfRange` error | — |
| main 2.0.0 | held (floor 0) | 122 | no | ok | **only 122 — loses 92..121** |
| PR #114 | held (floor 0) | 91 | no | ok | 91..122, zero loss |

The findings, in one line each:

- **The reservation holds for PR #114 too.** Its payload-only cursor does not
  ride the tracker (it stops at 91), but at age-out it is refused exactly like
  main — and it would be refused *earlier* (91 lags 122). The age-out
  behaviour akshatcoder-hash worried about is inherent to ANY slot cursor:
  once retention moves past the last thing the cursor names, the server must
  refuse, whatever the advancing rule is.
- **Main's cursor is not payload-only today** — on a quiet subscription it
  rides the SDK's internal slot tracker to the live head (122). So the
  proposed fix does not merely "keep the cursor current": it replaces a
  cursor that tracks the chain with one that tracks delivery, and buys the
  in-window recovery (91..122 vs main's loss of 92..121) at the price of
  aging out sooner.
- **The stall is invisible by default.** OUT_OF_RANGE is a transient error
  unless `intra_slot_updates` is set: with default options the consumer sees
  0 errors while the SDK silently retries the same refused slot every 5 s.
  The SDK already owns the switch that makes the failure loud; it is off.

Limits: mock geyser (window model = a single retention floor, real LaserStream
retention approximated, not measured against live infrastructure); Rust SDK
only; PR #114 applied to the 2.0.0 crate, not a live branch checkout.

Per-cell JSON: `research/laserstream-115-quiet-{aged,resume}-rule-{main,pr114}-{default-options,intra-slot-updates}-2026-11-20.json`,
matrix summary: `research/laserstream-115-quiet-subscription-matrix-2026-11-20.json`.
Regression: all twelve 2026-11-19 cells (main and PR #114, compliant-server
mode) reproduce IDENTICAL on the extended harness.

## The consumer-side clamp repair (2026-11-20)

The age-out reservation holds for ANY slot cursor — so the open design
question is not "which cursor rule" but "what does the consumer do when the
cursor is refused?". The SDK already ships three pieces that together form
the repair; this measurement asks whether they actually work.

Scenario (`SCENARIO=quiet-clamp`): payload@90, payload@91, ONE tick from the
SDK's internal tracker @122, then silence — and the chain advances to **136**
behind retention floor **130**, so the window evicted slots 92..129 (38 slots).

| cell | consumer options | consumer sees | outcome |
|---|---|---|---|
| silent | default | **nothing** (0 errors) | server refused twice; the SDK retried the stale cursor 122 forever — the stall, invisible |
| intra | `intra_slot_updates` | terminal `OutOfRange` | the SDK already owns the switch that makes the stall loud |
| repair | `intra_slot_updates` + clamp | the gap | 1 `subscribe_replay_info` query → `first_available=130` → re-subscribe at `from_slot=130` → delivery resumes 130..136, and **92..129 is declared as a gap** (38 slots) |
| control | `intra_slot_updates`, NO clamp | — | re-subscribing at the stale cursor 122 is refused AGAIN (3 refusals), 0 delivered |

The findings:

- **The repair is real and entirely consumer-side.** No SDK change, no server
  change: `subscribe_replay_info` + `intra_slot_updates` + an explicit clamped
  `from_slot` turn the silent infinite stall into a declared gap plus live
  delivery.
- **The cost is honest and permanent.** Slots 92..129 are unrecoverable — the
  repair can *name* them, never refill them. What the clamp buys is truth
  (a machine-readable gap) instead of silence.
- **The clamp is the operative rule, not the re-subscribe.** The control
  cell re-subscribes at the same stale cursor and is refused again. This is
  exactly what the SDK's own `subscribe_replay_info` docstring prescribes:
  *"Clamp with `from_slot.max(first_available)` and treat the skipped slots as
  missed."*
- **Default options make the repair unreachable.** The consumer never sees
  an error, so it never learns it should repair.

Per-cell JSON: `research/laserstream-115-quiet-clamp-{silent,intra,repair,control}-2026-11-20.json`,
summary: `research/laserstream-115-quiet-clamp-summary-2026-11-20.json`.

Regression: all eight quiet-matrix cells from the measurement above
reproduce IDENTICAL after this extension (only three additive diagnostic
keys: `chain_head_harness`, `first_available_reported`, `replay_info_queries`);
the quiet matrix summary is byte-identical.

Limits: mock geyser (window = one retention floor + a chain head); Rust SDK
only; `subscribe_replay_info` races eviction — a real server's floor can move
between the query and the fresh subscribe; the declared gap is the
consumer's own bookkeeping (the SDK has no gap primitive).

## Run

```bash
cargo run                      # non-compliant server: makes the skip visible
HONOR_FROM_SLOT=1 cargo run    # compliant server: what the consumer actually receives
SCENARIO=quiet cargo run       # quiet-subscription age-out (default options)
SCENARIO=quiet INTRA_SLOT_UPDATES=1 cargo run   # ... with intra_slot_updates
SCENARIO=quiet-clamp cargo run # the consumer-side clamp repair (4 cells)
# the PR #114 rule: build via .pr114-run (vendored patched crate) and
# MEASURE_RULE=pr114; see research/laserstream-115-quiet-subscription-matrix
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

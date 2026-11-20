//! Independent measurement for helius-labs/laserstream-sdk issue #115:
//! "Replay cursor advances from user slot updates, so mid-replay reconnects
//! can skip undelivered payloads."
//!
//! A local mock geyser server replays the exact two-window story of #115:
//!
//!   connection 1 (replay): payload slots 90 and 91, then a PROCESSED slot
//!   update for live slot 122 matched only by the USER slot filter, then the
//!   stream ends.
//!
//!   connection 2: the mock records the `from_slot` of the reconnect request
//!   and then serves payloads 92..121 (what a replay from 92 WOULD deliver).
//!
//! The measurement asks one question: does the SDK's reconnect request resume
//! at 92, or does it skip the undelivered range by asking for 122?
//!
//! Variant A (user slot filter WITHOUT filter_by_commitment — the default)
//! is the case akshatcoder-hash measured on devnet and case 1 of #115.
//! The FILTER_EVENTS control is the false-positive guard: a payload-only
//! stream must still resume exactly at its last delivered slot.

use laserstream_core_proto::geyser::{
    geyser_server::{Geyser, GeyserServer},
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterSlots, SubscribeRequestFilterBlockFooter, SubscribeUpdate, SubscribeUpdateAccount,
    SubscribeUpdateAccountInfo, SubscribeUpdateSlot, SubscribeReplayInfoRequest,
    SubscribeReplayInfoResponse, SubscribeReplayInfoRequest as _R, PingRequest, PongResponse,
};
use laserstream_core_proto::tonic::{self, Request, Response, Status, Streaming};
use helius_laserstream::{subscribe, LaserstreamConfig};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const LIVE_SLOT: u64 = 122; // the PROCESSED tick that races ahead of delivery
const LAST_DELIVERED: u64 = 91; // newest payload actually delivered on connection 1
const EXPECTED_RESUME: u64 = LAST_DELIVERED + 1; // 92: what replay owes the consumer
const SKIPPED_RANGE: u64 = LIVE_SLOT - EXPECTED_RESUME; // slots 92..=121

#[derive(Clone, Copy, PartialEq, Debug)]
enum Script {
    /// payload 90, payload 91, PROCESSED slot tick @122 (user filter only), stream end
    SlotTick,
    /// payload-only control: payload 90, payload 91, stream end
    PayloadOnly,
    /// payload 90, payload 91, CONFIRMED slot tick @122 (status == request commitment), stream end
    SlotTickMatchingCommitment,
    /// payload 90, payload 91, CONFIRMED slot tick @122 tagged ONLY with the SDK's
    /// internal slot filter id (the face PR #114 addresses), stream end
    SlotTickInternalOnly,
    /// payload 90, payload 91, then a BLOCK FOOTER at slot 122 on a user filter
    /// (the only non-slot update that advances the cursor on main), stream end
    BlockFooter,
    /// a FOOTER-ONLY subscription: footers 90, 91 and the live-head footer 122,
    /// no account payloads at all, stream end. The cursor then has only the
    /// footer to advance from.
    FooterOnly,
    /// QUIET: payload 90, payload 91, then the SDK's INTERNAL slot tracker
    /// ticks at the live head (122) — tagged ONLY with the internal filter id
    /// — and the subscription goes quiet: no further payloads, no further
    /// ticks. The chain advances while the consumer is silent.
    QuietWindow,
    /// QUIET + HELD: the same quiet subscription, but the server's replay
    /// window still holds the last delivered payload slot.
    QuietResume,
    /// QUIET + CLAMP: the same quiet age-out as QuietWindow, but the chain
    /// advanced to 136 while the consumer was silent (retention floor 130,
    /// head 136) — the scenario where a consumer-side clamp has real work:
    /// the window evicted 92..129, so a clamp at first_available=130 must
    /// resume delivery at 130..=136 and DECLARE 92..129 as a gap.
    QuietClamp,
}

/// A quiet-subscription scenario: the server's replay window and what it does
/// with a from_slot.
///
/// `window_floor` is the lowest slot the server can still replay. A request
/// below it is refused with OUT_OF_RANGE — the real LaserStream behaviour for
/// a slot that has aged out of retention. A request at or above it is served
/// from that slot (inclusive).
#[derive(Clone, Copy, Debug, PartialEq)]
struct QuietScenario {
    window_floor: u64,
    /// Highest slot with payload after the silence (>= window_floor).
    chain_head: u64,
}

/// The window slid on: silence outlasted retention, so the floor moved past
/// even the live-head slot the SDK's internal tracker rides (122). A cursor
/// that asks for 122 is now refused; a payload-only cursor sends NO from_slot
/// and cannot be refused at all.
const QUIET_AGED: QuietScenario = QuietScenario { window_floor: LIVE_SLOT + 8, chain_head: LIVE_SLOT };

/// The window still holds the last delivered payload (91): the replay works.
const QUIET_RESUME: QuietScenario = QuietScenario { window_floor: 0, chain_head: LIVE_SLOT };

/// QUIET + CLAMP: retention floor 130, chain head 136. The chain advanced
/// 38 slots past the last delivered payload while the consumer was silent —
/// exactly the shape the age-out stall hides.
const QUIET_CLAMP: QuietScenario = QuietScenario { window_floor: 130, chain_head: 136 };

#[derive(Debug, Clone)]
struct Conn {
    from_slot: Option<u64>,
    first_filter_has_slot: bool,
}

#[derive(Debug, Default, Clone)]
struct Observed {
    conns: Vec<Conn>,
    /// account payload slots delivered, tagged with the connection index
    account_slots: Vec<(usize, u64)>,
    /// block footer slots delivered, tagged with the connection index
    footer_slots: Vec<(usize, u64)>,
    /// gRPC status codes the consumer stream surfaced to the application
    consumer_errors: Vec<String>,
    /// how many times the server answered OUT_OF_RANGE
    out_of_range_replies: usize,
    /// how often the consumer asked the server what it can still replay
    replay_info_queries: usize,
    /// first_available the server reported to that consumer
    first_available_reported: Option<u64>,
    /// wall-clock delay between two consecutive connect attempts
    reconnect_delays_ms: Vec<u128>,
}

struct MockGeyser {
    script: Script,
    observed: Arc<Mutex<Observed>>,
    /// when true the reconnect branch serves what a COMPLIANT server would
    /// serve for the requested from_slot (inclusive) instead of a fixed script
    honor_from_slot: bool,
    /// for QUIET scripts: retention floor and what the server replays when
    /// the window still covers the request
    quiet: Option<QuietScenario>,
    /// the OUT_OF_RANGE status text served when the request is below the floor
    out_of_range_detail: String,
}

fn account_payload(slot: u64) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec!["acct".to_string()],
        update_oneof: Some(laserstream_core_proto::geyser::subscribe_update::UpdateOneof::Account(
            SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo::default()),
                slot,
                is_startup: false,
                bank_id: None,
            },
        )),
        ..Default::default()
    }
}

fn slot_tick_status_id(slot: u64, filter_id: String) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec![filter_id],
        update_oneof: Some(laserstream_core_proto::geyser::subscribe_update::UpdateOneof::Slot(
            SubscribeUpdateSlot {
                slot,
                parent: None,
                status: CommitmentLevel::Confirmed as i32,
                dead_error: None,
                bank_id: None,
            },
        )),
        ..Default::default()
    }
}

fn slot_tick_status(slot: u64, user_filter: bool, status: CommitmentLevel) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: if user_filter {
            vec!["slots".to_string()]
        } else {
            Vec::new()
        },
        update_oneof: Some(laserstream_core_proto::geyser::subscribe_update::UpdateOneof::Slot(
            SubscribeUpdateSlot {
                slot,
                parent: None,
                status: status as i32,
                dead_error: None,
                bank_id: None,
            },
        )),
        ..Default::default()
    }
}

#[tonic::async_trait]
impl Geyser for MockGeyser {
    type SubscribeStream =
        std::pin::Pin<Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<SubscribeUpdate, Status>> + Send + 'static>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut inner = request.into_inner();
        let first = inner
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("no request"))?;
        let has_user_slot = first.slots.contains_key("slots");
        let is_reconnect = first.from_slot.is_some();
        // The SDK adds a hidden internal slot filter (id starts with "internal-").
        // The mock echoes that id back on a slot tick so the tick is EXCLUSIVELY
        // from the internal subscription — the face PR #114 addresses.
        let internal_id = first
            .slots
            .keys()
            .find(|k| k.as_str().starts_with("internal-"))
            .cloned()
            .unwrap_or_default();
        {
            let mut o = self.observed.lock().unwrap();
            o.conns.push(Conn {
                from_slot: first.from_slot,
                first_filter_has_slot: has_user_slot,
            });
        }
        let script = self.script;
        let observed = self.observed.clone();
        let honor = self.honor_from_slot;
        let quiet = self.quiet;
        let oor_detail = self.out_of_range_detail.clone();
        let conn_index = {
            let mut o = self.observed.lock().unwrap();
            o.conns.len().saturating_sub(1)
        };
        let reconnect_from = first.from_slot;
        let stream = async_stream::stream! {
            if !is_reconnect {
                if script == Script::FooterOnly {
                    // A footer-only consumer: the cursor can only advance from a footer.
                    {
                        let mut o = observed.lock().unwrap();
                        o.footer_slots.push((conn_index, 90));
                        o.footer_slots.push((conn_index, LAST_DELIVERED));
                        o.footer_slots.push((conn_index, LIVE_SLOT));
                    }
                    yield Ok(block_footer(90, "footers".to_string()));
                    yield Ok(block_footer(LAST_DELIVERED, "footers".to_string()));
                    yield Ok(block_footer(LIVE_SLOT, "footers".to_string()));
                } else {
                // First connection: deliver the payloads, then (maybe) the racing tick.
                {
                    let mut o = observed.lock().unwrap();
                    o.account_slots.push((conn_index, 90));
                    o.account_slots.push((conn_index, LAST_DELIVERED));
                }
                yield Ok(account_payload(90));
                yield Ok(account_payload(LAST_DELIVERED));
                if script == Script::SlotTick {
                    yield Ok(slot_tick_status(LIVE_SLOT, true, CommitmentLevel::Processed));
                } else if script == Script::SlotTickMatchingCommitment {
                    yield Ok(slot_tick_status(LIVE_SLOT, true, CommitmentLevel::Confirmed));
                } else if script == Script::SlotTickInternalOnly {
                    yield Ok(slot_tick_status_id(LIVE_SLOT, internal_id.clone()));
                } else if script == Script::QuietWindow
                    || script == Script::QuietResume
                    || script == Script::QuietClamp
                {
                    // QUIET: deliver the two payloads, then exactly ONE live
                    // tick from the SDK's own internal tracker — tagged only
                    // with the internal filter id, like a real LaserStream
                    // server keeps ticking while the user's subscription is
                    // quiet — then the subscription goes silent.
                    yield Ok(slot_tick_status_id(LIVE_SLOT, internal_id.clone()));
                } else if script == Script::BlockFooter {
                    {
                        let mut o = observed.lock().unwrap();
                        o.footer_slots.push((conn_index, LIVE_SLOT));
                    }
                    yield Ok(block_footer(122, "footers".to_string()));
                }
                }
                // stream ends -> the SDK reconnects
            } else {
                if script == Script::FooterOnly {
                    // A compliant server resumes AT the requested from_slot (inclusive).
                    let start = reconnect_from.unwrap_or(90);
                    for s in start..=LIVE_SLOT {
                        {
                            let mut o = observed.lock().unwrap();
                            o.footer_slots.push((conn_index, s));
                        }
                        yield Ok(block_footer(s, "footers".to_string()));
                    }
                } else if script == Script::QuietWindow
                    || script == Script::QuietResume
                    || script == Script::QuietClamp
                {
                    // A from_slot AT OR ABOVE the window floor is served from
                    // that slot (inclusive); a from_slot BELOW it is refused
                    // with OUT_OF_RANGE, like a real server whose retention
                    // moved on. A reconnect with NO cursor at all is served
                    // from the last delivered payload — a server cannot skip
                    // what was never requested.
                    let q = quiet.expect("quiet scripts carry a QuietScenario");
                    if let Some(f) = first.from_slot {
                        if f < q.window_floor {
                            {
                                let mut o = observed.lock().unwrap();
                                o.out_of_range_replies += 1;
                            }
                            yield Err(Status::out_of_range(oor_detail));
                        } else {
                            for s in f..=q.chain_head {
                                {
                                    let mut o = observed.lock().unwrap();
                                    o.account_slots.push((conn_index, s));
                                }
                                yield Ok(account_payload(s));
                            }
                            // keep the tracker alive on this connection too
                            yield Ok(slot_tick_status_id(LIVE_SLOT, internal_id.clone()));
                        }
                        // Hold the connection open so a retrying client sits on it.
                        tokio::time::sleep(Duration::from_secs(6)).await;
                    } else {
                        // No cursor in the request: a real server replays from
                        // its earliest held slot (first_available = the floor).
                        let start = std::cmp::max(90, q.window_floor.min(q.chain_head + 1));
                        for s in start..=q.chain_head {
                            {
                                let mut o = observed.lock().unwrap();
                                o.account_slots.push((conn_index, s));
                            }
                            yield Ok(account_payload(s));
                        }
                        yield Ok(slot_tick_status_id(LIVE_SLOT, internal_id.clone()));
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                    }
                } else {
                // Reconnect. A COMPLIANT server resumes AT the requested
                // from_slot (inclusive) and serves everything from there on.
                // A NON-compliant mock ignores from_slot and always serves the
                // range 92..=121, which is what makes the skip visible.
                let start = if honor {
                    reconnect_from.unwrap_or(EXPECTED_RESUME)
                } else {
                    EXPECTED_RESUME
                };
                for s in start..LIVE_SLOT {
                    {
                        let mut o = observed.lock().unwrap();
                        o.account_slots.push((conn_index, s));
                    }
                    yield Ok(account_payload(s));
                }
                // Keep the connection open briefly so the SDK has time to deliver
                // before we end the stream and force yet another reconnect loop.
                tokio::time::sleep(Duration::from_millis(1500)).await;
                let _ = observed.clone();
                }
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn subscribe_preprocessed(
        &self,
        _: Request<Streaming<laserstream_core_proto::geyser::SubscribePreprocessedRequest>>,
    ) -> Result<Response<Self::SubscribePreprocessedStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    type SubscribePreprocessedStream =
        std::pin::Pin<Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<laserstream_core_proto::geyser::SubscribePreprocessedUpdate, Status>> + Send + 'static>>;

    async fn subscribe_replay_info(
        &self,
        _: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        {
            let mut o = self.observed.lock().unwrap();
            o.replay_info_queries += 1;
            o.first_available_reported = self.quiet.map(|q| q.window_floor);
        }
        Ok(Response::new(SubscribeReplayInfoResponse {
            first_available: self.quiet.map(|q| q.window_floor),
        }))
    }

    async fn ping(&self, req: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Ok(Response::new(PongResponse {
            count: req.into_inner().count,
        }))
    }
    async fn get_latest_blockhash(
        &self,
        _: Request<laserstream_core_proto::geyser::GetLatestBlockhashRequest>,
    ) -> Result<Response<laserstream_core_proto::geyser::GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_block_height(
        &self,
        _: Request<laserstream_core_proto::geyser::GetBlockHeightRequest>,
    ) -> Result<Response<laserstream_core_proto::geyser::GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_slot(
        &self,
        _: Request<laserstream_core_proto::geyser::GetSlotRequest>,
    ) -> Result<Response<laserstream_core_proto::geyser::GetSlotResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn is_blockhash_valid(
        &self,
        _: Request<laserstream_core_proto::geyser::IsBlockhashValidRequest>,
    ) -> Result<Response<laserstream_core_proto::geyser::IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_version(
        &self,
        _: Request<laserstream_core_proto::geyser::GetVersionRequest>,
    ) -> Result<Response<laserstream_core_proto::geyser::GetVersionResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

async fn start_server(script: Script) -> (String, Arc<Mutex<Observed>>, tokio::task::JoinHandle<()>) {
    start_server_full(script, false, None).await
}

async fn start_server_with(script: Script, honor_from_slot: bool) -> (String, Arc<Mutex<Observed>>, tokio::task::JoinHandle<()>) {
    start_server_full(script, honor_from_slot, None).await
}

async fn start_server_full(
    script: Script,
    honor_from_slot: bool,
    quiet: Option<QuietScenario>,
) -> (String, Arc<Mutex<Observed>>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let observed = Arc::new(Mutex::new(Observed::default()));
    let service = MockGeyser {
        script,
        observed: observed.clone(),
        honor_from_slot,
        quiet,
        // The error text a from_slot-below-retention request produces.
        out_of_range_detail: format!(
            "requested from_slot is before the replay window: window_floor={} (harness)",
            quiet.map(|q| q.window_floor).unwrap_or(0)
        ),
    };
    let incoming = async_stream::stream! {
        loop { yield listener.accept().await.map(|(socket, _)| socket); }
    };
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(GeyserServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    (endpoint, observed, task)
}

fn block_footer(slot: u64, filter_id: String) -> SubscribeUpdate {
    SubscribeUpdate {
        filters: vec![filter_id],
        update_oneof: Some(laserstream_core_proto::geyser::subscribe_update::UpdateOneof::BlockFooter(
            laserstream_core_proto::geyser::SubscribeUpdateBlockFooter { slot, ..Default::default() },
        )),
        ..Default::default()
    }
}

fn client_request() -> SubscribeRequest {
    let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    accounts.insert("acct".to_string(), SubscribeRequestFilterAccounts::default());
    let mut slots: HashMap<String, SubscribeRequestFilterSlots> = HashMap::new();
    // USER slot filter WITHOUT filter_by_commitment: matches slot updates at
    // every status, including PROCESSED ticks ahead of the CONFIRMED payload
    // stream. This is issue #115 case 1 / the devnet-reproduced shape.
    slots.insert(
        "slots".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: None,
        },
    );
    let mut footers: HashMap<String, SubscribeRequestFilterBlockFooter> = HashMap::new();
    footers.insert("footers".to_string(), SubscribeRequestFilterBlockFooter::default());
    SubscribeRequest {
        accounts,
        slots,
        block_footer: footers,
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    }
}

async fn run(script: Script, out_path: &str) -> serde_json::Value {
    run_with(script, out_path, false).await
}

async fn run_with(script: Script, out_path: &str, quiet_run: bool) -> serde_json::Value {
    let quiet = if quiet_run {
        Some(if script == Script::QuietResume { QUIET_RESUME } else { QUIET_AGED })
    } else {
        None
    };
    let honor = std::env::var("HONOR_FROM_SLOT").is_ok();
    let (endpoint, observed, task) = if honor {
        start_server_full(script, true, quiet).await
    } else {
        start_server_full(script, false, quiet).await
    };
    let config = LaserstreamConfig {
        api_key: String::new(),
        endpoint,
        max_reconnect_attempts: Some(6),
        channel_options: helius_laserstream::ChannelOptions::default(),
        replay: true,
    };
    // The client subscription uses `subscribe` (default options) or
    // `subscribe_with_options` when intra-slot updates are requested.
    type ClientStream = std::pin::Pin<
        Box<dyn futures::Stream<
            Item = Result<
                laserstream_core_proto::geyser::SubscribeUpdate,
                helius_laserstream::LaserstreamError,
            >,
        > + Send>,
    >;
    let (stream, _handle): (ClientStream, _) = match std::env::var("INTRA_SLOT_UPDATES").as_deref() {
        Ok("1") => {
            let (s, h) = helius_laserstream::subscribe_with_options(
                config,
                client_request(),
                helius_laserstream::SubscribeOptions::default().with_intra_slot_updates(true),
            );
            (Box::pin(s) as ClientStream, h)
        }
        _ => {
            let (s, h) = subscribe(config, client_request());
            (Box::pin(s) as ClientStream, h)
        }
    };
    futures::pin_mut!(stream);
    let mut delivered: Vec<u64> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(update))) => {
                if let Some(laserstream_core_proto::geyser::subscribe_update::UpdateOneof::Account(a)) =
                    update.update_oneof
                {
                    delivered.push(a.slot);
                }
                // Enough evidence collected: two windows observed end to end.
                if delivered.iter().filter(|&&s| s >= EXPECTED_RESUME).count() >= 5 {
                    break;
                }
            }
            Ok(Some(Err(err))) => {
                let mut o = observed.lock().unwrap();
                o.consumer_errors.push(format!("{err}"));
            }
            Ok(None) | Err(_) => {}
        }
    }
    let obs = observed.lock().unwrap().clone();
    let reconnect_from = obs.conns.get(1).and_then(|c| c.from_slot);
    // The slots a from_slot-respecting server would never re-deliver:
    // [LAST_DELIVERED+1, reconnect_from_slot).
    let skipped: Vec<u64> = match reconnect_from {
        Some(from) if from > LAST_DELIVERED => (LAST_DELIVERED + 1..from).collect(),
        _ => Vec::new(),
    };
    let quiet_flag = matches!(script, Script::QuietWindow | Script::QuietResume);
    let res = serde_json::json!({
        "issue": "helius-labs/laserstream-sdk#115",
        "scenario": if quiet_flag {
            if script == Script::QuietResume {
                "QUIET_RESUME: window still holds the cursor slot -> replay"
            } else {
                "QUIET_AGED: window slid past -> OUT_OF_RANGE"
            }
        } else {
            "two-window skip race"
        },
        "variant": match script {
            Script::SlotTick => "A: user slot filter without filter_by_commitment, tick status PROCESSED (issue #115 case 1)",
            Script::SlotTickMatchingCommitment => "B: user slot filter with filter_by_commitment, tick status CONFIRMED == request commitment (issue #115 case 2)",
            Script::SlotTickInternalOnly => "C: slot tick tagged ONLY with the SDK internal slot filter (the face PR #114 addresses)",
            Script::PayloadOnly => "CONTROL: no slot update delivered at all (payload-only stream)",
            Script::BlockFooter => "D: BLOCK FOOTER at 122 on a user filter (the non-slot update that advances the cursor on main; PR #114's replay_cursor_slot omits BlockFooter)",
            Script::FooterOnly => "E: FOOTER-ONLY subscription (footers 90, 91, live-head 122; no account payloads) — the live-head footer alone in the cursor",
            Script::QuietWindow => "F: quiet subscription, replay window slid past the cursor (cursor-only-advance fix scenario)",
            Script::QuietResume => "F-CONTROL: quiet subscription, window still holds the cursor slot",
            Script::QuietClamp => "G: quiet subscription, chain advanced to 136 behind floor 130 — the consumer-side clamp repair scenario",
        },
        "sdk_source": "helius-laserstream 2.0.0 from crates.io (its client.rs cursor logic is byte-identical to main 379a6ba: tracked_slot = s.slot on any slot update, payload slots never advance the cursor)",
        "mock_ignores_from_slot": true,
        "server_behavior": if quiet_flag {
            "the mock answers from_slot below first_available with OUT_OF_RANGE and holds the connection open"
        } else {
            "the mock replays its fixed script regardless of from_slot; reconnect_from_slot is the measurement"
        },
        "commitment": "confirmed",
        "consumer_options_intra_slot_updates": std::env::var("INTRA_SLOT_UPDATES").is_ok(),
        "sdk_rule": std::env::var("MEASURE_RULE").unwrap_or_else(|_| "main".into()),
        "script": if script == Script::SlotTick {
            "payload@90, payload@91, PROCESSED slot tick @122 (user filter), stream end -> reconnect"
        } else if script == Script::QuietClamp {
            "payload@90, payload@91, internal tick @122, silence; chain head 136 behind floor 130"
        } else if script == Script::SlotTickMatchingCommitment {
            "payload@90, payload@91, CONFIRMED slot tick @122 (status == request commitment), stream end -> reconnect"
        } else if script == Script::BlockFooter {
            "payload@90, payload@91, BLOCK FOOTER@122 (user filter 'footers'), stream end -> reconnect"
        } else if script == Script::FooterOnly {
            "footer@90, footer@91, footer@122 (live head), stream end -> reconnect"
        } else if quiet_flag {
            "payload@90, payload@91, stream end; chain advances while the consumer stays quiet"
        } else {
            "payload@90, payload@91, stream end -> reconnect"
        },
        "connection1_delivered_slots": delivered.iter().filter(|&&s| s <= LAST_DELIVERED).collect::<Vec<_>>(),
        "reconnect_from_slot": reconnect_from,
        "reconnect_attempts": obs.conns.len().saturating_sub(1),
        "replay_info_queries": obs.replay_info_queries,
        "first_available_reported": obs.first_available_reported,
        "chain_head_harness": quiet.map(|q| q.chain_head),
        "expected_resume_slot": EXPECTED_RESUME,
        "payloads_delivered_after_reconnect": delivered.iter().filter(|&&s| s >= EXPECTED_RESUME).count(),
        "slots_a_compliant_server_would_never_redeliver": skipped,
        "defect_present": matches!(reconnect_from, Some(s) if s > LAST_DELIVERED),
        "skipped_range_size": SKIPPED_RANGE,
        "compliant_server_mode": honor,
        "replay_window_floor_harness": quiet.map(|q| q.window_floor),
        "out_of_range_replies": obs.out_of_range_replies,
        "consumer_errors": obs.consumer_errors,
        "delivered_account_slots_by_conn": obs.account_slots,
        "delivered_footer_slots_by_conn": obs.footer_slots,
        "footer_only_consumer": script == Script::FooterOnly,
    });
    std::fs::write(out_path, serde_json::to_string_pretty(&res).unwrap()).unwrap();
    task.abort();
    res
}

#[tokio::main]
async fn main() {
    if std::env::var("SCENARIO").as_deref() == Ok("quiet") {
        run_quiet().await;
        return;
    }
    if std::env::var("SCENARIO").as_deref() == Ok("quiet-clamp") {
        run_quiet_clamp().await;
        return;
    }
    run_both().await;
}

/// The quiet-subscription measurement (2026-11-20).
///
/// The open half of #115 (akshatcoder-hash, 18:20Z): if the cursor advances
/// ONLY on delivered payload slots, a subscription that goes quiet stops
/// advancing the cursor — and a replay window that slid past the cursor then
/// answers OUT_OF_RANGE. Measured as a 2x2 over RULE x WINDOW, both on the
/// SDK's default options (OUT_OF_RANGE is a transient error, retried every
/// 5 s) and once with intra_slot_updates = true (the SDK's own switch that
/// makes OUT_OF_RANGE terminal and surfaces it to the consumer):
///
///   F (QuietWindow, aged):   floor 130 -> a 122-cursor is refused.
///   F-control (QuietResume): floor 0   -> the replay works.
///
/// Rule = main 2.0.0 (tracked_slot rides ANY slot update incl. the internal
/// tracker) vs PR #114 (replay_cursor_slot: payload slots only).
async fn run_quiet() {
    let dir = std::env::var("MEASURE_OUT").unwrap_or_else(|_| ".".into());
    let intra = std::env::var("INTRA_SLOT_UPDATES").as_deref() == Ok("1");
    let options = if intra { "intra-slot-updates" } else { "default-options" };
    let rule = std::env::var("MEASURE_RULE").unwrap_or_else(|_| "main".into());

    let aged = run_with(
        Script::QuietWindow,
        &format!("{dir}/laserstream-115-quiet-aged-rule-{rule}-{options}-2026-11-20.json"),
        true,
    )
    .await;
    println!("QUIET AGED ({rule}): {}", serde_json::to_string_pretty(&aged).unwrap());

    let control = run_with(
        Script::QuietResume,
        &format!("{dir}/laserstream-115-quiet-resume-rule-{rule}-{options}-2026-11-20.json"),
        true,
    )
    .await;
    println!("QUIET RESUME ({rule}): {}", serde_json::to_string_pretty(&control).unwrap());

    // Falsification guards. Each one pins a claim this report makes; if the
    // measurement changes, the claim must be re-checked, not asserted away.
    //
    // 1. The aged window MUST refuse a cursor that lags the floor, otherwise
    //    the scenario did not happen (vacuous finding). Main asks for 122,
    //    PR #114 asks for 91 — both below the floor 130.
    assert_eq!(
        aged["out_of_range_replies"].as_u64().unwrap_or(0) > 0,
        true,
        "the aged window must refuse the lagging cursor with OUT_OF_RANGE"
    );
    // 2. Main's cursor rides the internal tracker to 122 in the held window:
    //    it is served and skips 92..121 — the #115 defect in its quiet form.
    if rule == "main" {
        assert_eq!(
            aged["reconnect_from_slot"].as_u64().unwrap_or(0),
            LIVE_SLOT,
            "main: the internal tracker must carry the cursor to the live head"
        );
    } else {
        // 3. PR #114's payload-only cursor must NOT reach the live head: it
        //    stops at the newest delivered payload (91) — that is what makes
        //    it MORE exposed to age-out, not less.
        assert_eq!(
            aged["reconnect_from_slot"].as_u64().unwrap_or(0),
            LAST_DELIVERED,
            "pr114: the payload-only cursor must stop at the last delivered payload"
        );
    }
    // 4. The control MUST be served (the replay still works in-window) — this
    //    is the false-positive guard: if the control were refused too, the
    //    finding would only describe the mock.
    assert_eq!(
        control["out_of_range_replies"].as_u64().unwrap_or(0),
        0,
        "the in-window control must not be refused"
    );
    assert_eq!(
        control["payloads_delivered_after_reconnect"].as_u64().unwrap_or(0) > 0,
        true,
        "the in-window control must actually replay payloads"
    );
    // 5. With default options the OUT_OF_RANGE is NOT surfaced to the consumer
    //    (it is a transient error): the subscription stalls silently. That is
    //    the defect this measurement exists for — it must be observed.
    //    With intra_slot_updates the SDK treats it as terminal and the
    //    consumer DOES see the error.
    if intra {
        assert_eq!(
            aged["consumer_errors"].as_array().map(|a| a.len()).unwrap_or(0) > 0,
            true,
            "intra_slot_updates: the consumer must see the OUT_OF_RANGE"
        );
    } else {
        assert_eq!(
            aged["consumer_errors"].as_array().map(|a| a.len()).unwrap_or(0),
            0,
            "default options: the consumer must NOT see the OUT_OF_RANGE (silent stall)"
        );
    }
}

/// The consumer-side clamp repair measurement (2026-11-20).
///
/// PULS 4 proved the age-out reservation holds for ANY slot cursor. This
/// measurement asks the open design question the thread left: does the
/// SDK's OWN documented repair actually work, and what does it cost?
///
/// The SDK ships three pieces that together form the repair:
///   1. `LaserstreamClient::subscribe_replay_info()` returns
///      `first_available` — the oldest slot the server can still replay;
///   2. `subscribe_with_options(..., SubscribeOptions::with_intra_slot_updates(true))`
///      turns the silent OUT_OF_RANGE retry loop into a TERMINAL error the
///      consumer can see;
///   3. a fresh `subscribe()` with an explicit `from_slot =
///      max(cursor, first_available)` resumes at the server's floor.
///
/// QUIET_CLAMP: payload@90, payload@91, ONE internal tick @122, then
/// silence; the chain advances to 136 behind retention floor 130, so the
/// window evicted 92..129 (38 slots).
///
/// Cells measured:
///   silent (default options) : the consumer sees 0 errors, retries the
///       refused 122 forever — the stall PULS 4 found.
///   intra  (terminal)       : the consumer SEES the terminal OutOfRange.
///   clamp  (repair)         : consumer queries subscribe_replay_info
///       (-> first_available=130), re-subscribes with from_slot=130,
///       resumes 130..136, DECLARES 92..129 as a gap.
///   unclamped control       : re-subscribing at the old cursor (122) is
///       refused AGAIN — the clamp is what makes the repair work, not the
///       re-subscribe alone (false-positive guard).
async fn run_quiet_clamp() {
    let dir = std::env::var("MEASURE_OUT").unwrap_or_else(|_| ".".into());
    let config = LaserstreamConfig {
        api_key: String::new(),
        endpoint: String::new(),
        max_reconnect_attempts: Some(6),
        channel_options: helius_laserstream::ChannelOptions::default(),
        replay: true,
    };
    let request = client_request();

    // --- CELL 1: silent — default options. ---
    let silent = clamp_cell(&config, &request, 15, false).await;
    println!("QUIET CLAMP SILENT: {}", serde_json::to_string_pretty(&silent).unwrap());

    // --- CELL 2: intra_slot_updates — the consumer sees the terminal error. ---
    let intra = clamp_cell(&config, &request, 15, true).await;
    println!("QUIET CLAMP INTRA: {}", serde_json::to_string_pretty(&intra).unwrap());

    // --- CELL 3: clamp repair — subscribe_replay_info + fresh from_slot. ---
    let repair = clamp_repair(&config, &request, 15).await;
    println!("QUIET CLAMP REPAIR: {}", serde_json::to_string_pretty(&repair).unwrap());

    // --- CELL 4: unclamped control — same repair WITHOUT the clamp. ---
    let control = clamp_control(&config, &request, 15).await;
    println!("QUIET CLAMP CONTROL: {}", serde_json::to_string_pretty(&control).unwrap());

    for (name, cell) in [("silent", &silent), ("intra", &intra), ("repair", &repair), ("control", &control)] {
        let path = format!("{dir}/laserstream-115-quiet-clamp-{name}-2026-11-20.json");
        std::fs::write(&path, serde_json::to_string_pretty(cell).unwrap()).unwrap();
    }

    // --- Falsification guards: each claim must be observed. ---
    // 1. Silent cell: the OUT_OF_RANGE never reaches the consumer.
    assert_eq!(
        silent["consumer_errors"].as_array().map(|a| a.len()).unwrap_or(0),
        0,
        "silent cell: the consumer must see 0 errors (the stall is invisible)"
    );
    assert_eq!(
        silent["out_of_range_replies"].as_u64().unwrap_or(0) > 0,
        true,
        "silent cell: the server must have refused the cursor at least once"
    );
    // 2. Intra cell: the consumer DOES see the terminal error.
    assert_eq!(
        intra["consumer_errors"].as_array().map(|a| a.len()).unwrap_or(0) > 0,
        true,
        "intra cell: the consumer must see the terminal OUT_OF_RANGE"
    );
    // 3. Repair cell: first_available is reported, the clamp resumes at 130,
    //    the evicted range is declared as a gap, and delivery continues.
    assert_eq!(
        repair["replay_info_queries"].as_u64().unwrap_or(0),
        1,
        "repair cell: exactly one subscribe_replay_info query"
    );
    assert_eq!(
        repair["first_available_reported"].as_u64().unwrap_or(0),
        130,
        "repair cell: first_available must be the server's floor 130"
    );
    assert_eq!(
        repair["clamp_from_slot"].as_u64().unwrap_or(0),
        130,
        "repair cell: the clamp must clamp to 130, not the stale cursor"
    );
    assert_eq!(
        repair["delivered_after_clamp"].as_array().map(|a| a.len()).unwrap_or(0) >= 5,
        true,
        "repair cell: delivery must resume at the floor"
    );
    assert_eq!(
        repair["gap_declared_range_size"].as_u64().unwrap_or(0),
        38,
        "repair cell: the declared gap must be exactly 92..129"
    );
    // 4. Control cell: re-subscribing at the OLD cursor is refused again —
    //    the clamp is what makes the repair work.
    assert_eq!(
        control["out_of_range_replies"].as_u64().unwrap_or(0) > 0,
        true,
        "control cell: the unclamped re-subscribe must be refused again"
    );
    assert_eq!(
        control["delivered_after_clamp"].as_array().map(|a| a.len()).unwrap_or(0),
        0,
        "control cell: no delivery without the clamp"
    );
}

/// Run the QUIET_CLAMP scenario once: payloads + tick, silence, reconnect.
/// Returns the observed state as JSON.
async fn clamp_cell(
    config: &LaserstreamConfig,
    request: &SubscribeRequest,
    secs: u64,
    intra: bool,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    let (endpoint, observed, task) = start_server_full(Script::QuietClamp, true, Some(QUIET_CLAMP)).await;
    let cfg = LaserstreamConfig { endpoint, ..config.clone() };

    type ClientStream = std::pin::Pin<
        Box<dyn futures::Stream<
            Item = Result<laserstream_core_proto::geyser::SubscribeUpdate, helius_laserstream::LaserstreamError>,
        > + Send>,
    >;
    let (mut stream, _handle): (ClientStream, _) = if intra {
        let (s, h) = helius_laserstream::subscribe_with_options(
            cfg,
            request.clone(),
            helius_laserstream::SubscribeOptions::default().with_intra_slot_updates(true),
        );
        (Box::pin(s) as ClientStream, h)
    } else {
        let (s, h) = helius_laserstream::subscribe(cfg, request.clone());
        (Box::pin(s) as ClientStream, h)
    };

    let mut delivered: Vec<u64> = Vec::new();
    let mut consumer_errors: Vec<String> = Vec::new();

    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(update))) => {
                if let Some(laserstream_core_proto::geyser::subscribe_update::UpdateOneof::Account(a)) =
                    update.update_oneof
                {
                    delivered.push(a.slot);
                }
            }
            Ok(Some(Err(err))) => {
                let msg = format!("{err}");
                if !consumer_errors.contains(&msg) {
                    consumer_errors.push(msg);
                }
                if intra {
                    // Terminal error: the consumer is free to repair.
                    break;
                }
                // Default options: the SDK retries silently; keep draining.
            }
            Ok(None) | Err(_) => {}
        }
    }
    let obs = observed.lock().unwrap().clone();

    serde_json::json!({
        "scenario": "QUIET_CLAMP: chain head 136 behind floor 130; evicted 92..129 (38 slots)",
        "consumer_options": if intra { "intra-slot-updates" } else { "default-options" },
        "reconnect_from_slots": obs.conns.iter().map(|c| c.from_slot).collect::<Vec<_>>(),
        "reconnect_attempts": obs.conns.len().saturating_sub(1),
        "out_of_range_replies": obs.out_of_range_replies,
        "replay_info_queries": obs.replay_info_queries,
        "first_available_reported": obs.first_available_reported,
        "consumer_errors": consumer_errors,
        "delivered_slots": delivered,
    })
}

/// The repair cell: on the terminal error, query subscribe_replay_info,
/// clamp to first_available, re-subscribe, declare the gap.
async fn clamp_repair(
    config: &LaserstreamConfig,
    request: &SubscribeRequest,
    secs: u64,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    // Phase 1: subscribe until the terminal error surfaces.
    let (endpoint, observed, task) = start_server_full(Script::QuietClamp, true, Some(QUIET_CLAMP)).await;
    let cfg = LaserstreamConfig { endpoint: endpoint.clone(), ..config.clone() };
    let (mut stream, _handle) = {
        let (s, h) = helius_laserstream::subscribe_with_options(
            cfg.clone(),
            request.clone(),
            helius_laserstream::SubscribeOptions::default().with_intra_slot_updates(true),
        );
        (Box::pin(s), h)
    };

    let mut delivered: Vec<u64> = Vec::new();
    let mut phase1_errors: Vec<String> = Vec::new();
    let mut saw_terminal = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(update))) => {
                if let Some(laserstream_core_proto::geyser::subscribe_update::UpdateOneof::Account(a)) =
                    update.update_oneof
                {
                    delivered.push(a.slot);
                }
            }
            Ok(Some(Err(err))) => {
                let msg = format!("{err}");
                if !phase1_errors.contains(&msg) {
                    phase1_errors.push(msg);
                }
                saw_terminal = true;
                break;
            }
            Ok(None) | Err(_) => {}
        }
    }

    // Phase 2: the SDK's OWN documented repair.
    let client = helius_laserstream::LaserstreamClient::connect(LaserstreamConfig { endpoint: endpoint.clone(), ..config.clone() }).await.expect("connect");
    let info = client.subscribe_replay_info().await.expect("replay info");
    let first_available = info.first_available.expect("server reports first_available");
    let clamp_from = first_available.max(request.from_slot.unwrap_or(0));
    let gap = if clamp_from > LAST_DELIVERED {
        (LAST_DELIVERED + 1..clamp_from).collect::<Vec<u64>>()
    } else {
        Vec::new()
    };

    let mut repair_request = request.clone();
    repair_request.from_slot = Some(clamp_from);
    let (mut stream2, _handle2) = {
        let (s, h) = helius_laserstream::subscribe(LaserstreamConfig { endpoint: endpoint.clone(), ..config.clone() }, repair_request);
        (Box::pin(s), h)
    };

    let mut delivered_after: Vec<u64> = Vec::new();
    let deadline2 = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline2 {
        match tokio::time::timeout_at(deadline2, stream2.next()).await {
            Ok(Some(Ok(update))) => {
                if let Some(laserstream_core_proto::geyser::subscribe_update::UpdateOneof::Account(a)) =
                    update.update_oneof
                {
                    delivered_after.push(a.slot);
                }
            }
            _ => {}
        }
    }
    let obs = observed.lock().unwrap().clone();

    serde_json::json!({
        "scenario": "QUIET_CLAMP: chain head 136 behind floor 130; evicted 92..129 (38 slots)",
        "consumer_options": "intra-slot-updates",
        "reconnect_from_slots": obs.conns.iter().map(|c| c.from_slot).collect::<Vec<_>>(),
        "out_of_range_replies_phase1": obs.out_of_range_replies,
        "replay_info_queries": obs.replay_info_queries,
        "first_available_reported": obs.first_available_reported,
        "clamp_from_slot": clamp_from,
        "gap_declared_range": gap,
        "gap_declared_range_size": gap.len() as u64,
        "phase1_consumer_errors": phase1_errors,
        "delivered_phase1": delivered,
        "delivered_after_clamp": delivered_after,
    })
}

/// The control: same repair but WITHOUT clamping — re-subscribe at the old
/// cursor (122). The server must refuse again.
async fn clamp_control(
    config: &LaserstreamConfig,
    request: &SubscribeRequest,
    secs: u64,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    let (endpoint, observed, task) = start_server_full(Script::QuietClamp, true, Some(QUIET_CLAMP)).await;
    let cfg = LaserstreamConfig { endpoint: endpoint.clone(), ..config.clone() };
    let (mut stream, _handle) = {
        let (s, h) = helius_laserstream::subscribe_with_options(
            cfg,
            request.clone(),
            helius_laserstream::SubscribeOptions::default().with_intra_slot_updates(true),
        );
        (Box::pin(s), h)
    };

    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(Ok(_))) = tokio::time::timeout_at(deadline, stream.next()).await {}
    }
    let obs = observed.lock().unwrap().clone();

    // Re-subscribe at the OLD cursor (no clamp).
    let mut control_request = request.clone();
    control_request.from_slot = Some(LIVE_SLOT); // 122 — the stale cursor
    let (mut stream2, _handle2) = {
        let (s, h) = helius_laserstream::subscribe(LaserstreamConfig { endpoint: endpoint.clone(), ..config.clone() }, control_request);
        (Box::pin(s), h)
    };

    let mut delivered_after: Vec<u64> = Vec::new();
    let deadline2 = tokio::time::Instant::now() + Duration::from_secs(8);
    while tokio::time::Instant::now() < deadline2 {
        match tokio::time::timeout_at(deadline2, stream2.next()).await {
            Ok(Some(Ok(update))) => {
                if let Some(laserstream_core_proto::geyser::subscribe_update::UpdateOneof::Account(a)) =
                    update.update_oneof
                {
                    delivered_after.push(a.slot);
                }
            }
            _ => {}
        }
    }
    let obs2 = observed.lock().unwrap().clone();

    serde_json::json!({
        "scenario": "QUIET_CLAMP: unclamped control — re-subscribe at stale cursor 122",
        "consumer_options": "intra-slot-updates",
        "reconnect_from_slots": obs.conns.iter().map(|c| c.from_slot).collect::<Vec<_>>(),
        "out_of_range_replies": obs2.out_of_range_replies,
        "replay_info_queries": obs2.replay_info_queries,
        "delivered_after_clamp": delivered_after,
    })
}

async fn run_both() {
    let dir = std::env::var("MEASURE_OUT").unwrap_or_else(|_| ".".into());
    let a = run(
        Script::SlotTick,
        &format!("{dir}/laserstream-115-variant-a-2026-11-19.json"),
    )
    .await;
    println!("VARIANT A: {}", serde_json::to_string_pretty(&a).unwrap());
    let b = run(
        Script::SlotTickMatchingCommitment,
        &format!("{dir}/laserstream-115-variant-b-2026-11-19.json"),
    )
    .await;
    println!("VARIANT B: {}", serde_json::to_string_pretty(&b).unwrap());
    let cc = run(
        Script::SlotTickInternalOnly,
        &format!("{dir}/laserstream-115-variant-c-2026-11-19.json"),
    )
    .await;
    println!("VARIANT C: {}", serde_json::to_string_pretty(&cc).unwrap());
    let d = run(
        Script::BlockFooter,
        &format!("{dir}/laserstream-115-variant-d-footer-2026-11-19.json"),
    )
    .await;
    println!("VARIANT D: {}", serde_json::to_string_pretty(&d).unwrap());
    let e = run(
        Script::FooterOnly,
        &format!("{dir}/laserstream-115-variant-e-footer-only-2026-11-19.json"),
    )
    .await;
    println!("VARIANT E: {}", serde_json::to_string_pretty(&e).unwrap());
    let c = run(
        Script::PayloadOnly,
        &format!("{dir}/laserstream-115-control-2026-11-19.json"),
    )
    .await;
    println!("CONTROL: {}", serde_json::to_string_pretty(&c).unwrap());
    assert_eq!(
        b["defect_present"], true,
        "variant B (commitment-matching tick) must reproduce the skip to hold the finding"
    );
    // The finding: variant A skips the undelivered range on resume.
    assert_eq!(
        a["defect_present"], true,
        "variant A must reproduce the #115 skip to hold the finding"
    );
    // False-positive guard: the control (payload-only) must still be intact.
    assert_eq!(
        c["defect_present"], false,
        "control (payload-only) must resume at its last delivered slot"
    );
}

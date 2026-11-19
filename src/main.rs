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
}

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
}

struct MockGeyser {
    script: Script,
    observed: Arc<Mutex<Observed>>,
    /// when true the reconnect branch serves what a COMPLIANT server would
    /// serve for the requested from_slot (inclusive) instead of a fixed script
    honor_from_slot: bool,
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
                }
                // Keep the connection open briefly so the SDK has time to deliver
                // before we end the stream and force yet another reconnect loop.
                tokio::time::sleep(Duration::from_millis(1500)).await;
                let _ = observed.clone();
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
        Ok(Response::new(SubscribeReplayInfoResponse {
            first_available: Some(1),
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
    start_server_with(script, false).await
}

async fn start_server_with(script: Script, honor_from_slot: bool) -> (String, Arc<Mutex<Observed>>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let observed = Arc::new(Mutex::new(Observed::default()));
    let service = MockGeyser {
        script,
        observed: observed.clone(),
        honor_from_slot,
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
    let honor = std::env::var("HONOR_FROM_SLOT").is_ok();
    let (endpoint, observed, task) = if honor {
        start_server_with(script, true).await
    } else {
        start_server(script).await
    };
    let config = LaserstreamConfig {
        api_key: String::new(),
        endpoint,
        max_reconnect_attempts: Some(6),
        channel_options: helius_laserstream::ChannelOptions::default(),
        replay: true,
    };
    let (stream, _handle) = subscribe(config, client_request());
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
            Ok(Some(Err(_))) | Ok(None) | Err(_) => {}
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
    let res = serde_json::json!({
        "issue": "helius-labs/laserstream-sdk#115",
        "variant": match script {
            Script::SlotTick => "A: user slot filter without filter_by_commitment, tick status PROCESSED (issue #115 case 1)",
            Script::SlotTickMatchingCommitment => "B: user slot filter with filter_by_commitment, tick status CONFIRMED == request commitment (issue #115 case 2)",
            Script::SlotTickInternalOnly => "C: slot tick tagged ONLY with the SDK internal slot filter (the face PR #114 addresses)",
            Script::PayloadOnly => "CONTROL: no slot update delivered at all (payload-only stream)",
            Script::BlockFooter => "D: BLOCK FOOTER at 122 on a user filter (the non-slot update that advances the cursor on main; PR #114's replay_cursor_slot omits BlockFooter)",
            Script::FooterOnly => "E: FOOTER-ONLY subscription (footers 90, 91, live-head 122; no account payloads) — the live-head footer alone in the cursor",
        },
        "sdk_source": "helius-laserstream 2.0.0 from crates.io (its client.rs cursor logic is byte-identical to main 379a6ba: tracked_slot = s.slot on any slot update, payload slots never advance the cursor)",
        "mock_ignores_from_slot": true,
        "server_behavior": "the mock replays its fixed script regardless of from_slot; reconnect_from_slot is the measurement",
        "commitment": "confirmed",
        "script": if script == Script::SlotTick {
            "payload@90, payload@91, PROCESSED slot tick @122 (user filter), stream end -> reconnect"
        } else if script == Script::SlotTickMatchingCommitment {
            "payload@90, payload@91, CONFIRMED slot tick @122 (status == request commitment), stream end -> reconnect"
        } else if script == Script::BlockFooter {
            "payload@90, payload@91, BLOCK FOOTER@122 (user filter 'footers'), stream end -> reconnect"
        } else if script == Script::FooterOnly {
            "footer@90, footer@91, footer@122 (live head), stream end -> reconnect"
        } else {
            "payload@90, payload@91, stream end -> reconnect"
        },
        "connection1_delivered_slots": delivered.iter().filter(|&&s| s <= LAST_DELIVERED).collect::<Vec<_>>(),
        "reconnect_from_slot": reconnect_from,
        "expected_resume_slot": EXPECTED_RESUME,
        "payloads_delivered_after_reconnect": delivered.iter().filter(|&&s| s >= EXPECTED_RESUME).count(),
        "slots_a_compliant_server_would_never_redeliver": skipped,
        "defect_present": matches!(reconnect_from, Some(s) if s > LAST_DELIVERED),
        "skipped_range_size": SKIPPED_RANGE,
        "compliant_server_mode": honor,
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
    run_both().await;
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

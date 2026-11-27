//! One-direction walk: simulate packet forwarding hop by hop, replicating
//! ironwood's `_lookup` decision at every node (see src/routing.rs).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use crate::admin::AdminClient;
use crate::events::{Dir, Event, EventTx};
use crate::model::NodeCache;
use crate::publicpeers::PublicPeers;
use crate::routing::{candidates, classify, hop_trust, select_next, TrustLevel};
use crate::ygg::{tree_dist, Coords, Key};

#[derive(Clone)]
pub struct WalkParams {
    pub dir: Dir,
    pub self_key: Key,
    /// Where the simulated packet starts (self for forward, dest for reverse).
    pub start: Key,
    /// Where it is addressed to (dest for forward, self for reverse).
    pub target: Key,
    pub target_coords: Coords,
    pub max_hops: usize,
    pub deadline: Instant,
    /// Costs (ms) of our own links, from local getPeers; used for hop 1.
    pub self_link_costs: HashMap<Key, u64>,
    pub prefetch: bool,
    /// Fetch nodeinfo for each confirmed hop (--nodeinfo).
    pub fetch_nodeinfo: bool,
    /// Never skip the bulk peer scan even when hinted probes already found
    /// a candidate (--exact).
    pub exact: bool,
    /// How many times the target coords may be re-learned mid-walk.
    pub relearn_budget: u32,
}

/// Extract build name/version from a getNodeInfo response envelope
/// ({ "<key hex>": { ...nodeinfo... } }).
fn nodeinfo_strings(v: &serde_json::Value) -> (Option<String>, Option<String>) {
    let inner = v.as_object().and_then(|m| m.values().next());
    let get = |f: &str| {
        inner
            .and_then(|i| i.get(f))
            .and_then(|x| x.as_str())
            .map(str::to_string)
    };
    (get("buildname"), get("buildversion"))
}

pub struct WalkCtx {
    pub params: WalkParams,
    pub cache: Arc<NodeCache>,
    pub admin: Arc<AdminClient>,
    pub events: EventTx,
    pub cancel: CancellationToken,
    /// Probe-priority hints: the destination's tree view (its ancestry and
    /// peers) plus our own tree keys. Empty set degrades to a full scan.
    pub hints: Arc<std::collections::HashSet<Key>>,
    /// Known public-node endpoints (key → host); empty means no labels.
    pub public: Arc<PublicPeers>,
}

/// Fire-and-forget prefetch of the next hop's peer list.
fn prefetch_peers(ctx: &Arc<WalkCtx>, key: Key) {
    let cache = ctx.cache.clone();
    tokio::spawn(async move {
        let _ = cache.ensure_peers(key).await;
    });
}

pub async fn walk(ctx: Arc<WalkCtx>) {
    let p = &ctx.params;
    let started = Instant::now();
    let mut cur = p.start;
    let mut watermark = u64::MAX;
    let mut n: usize = 0;
    let mut trust = TrustTally::default();

    loop {
        if ctx.cancel.is_cancelled() || Instant::now() >= p.deadline {
            emit_verdict(&ctx, &trust, false).await;
            let _ = ctx
                .events
                .send(Event::DeadEnd {
                    dir: p.dir,
                    at: cur,
                    coords: ctx.cache.get_node(&cur).and_then(|d| d.coords),
                    msg: "cancelled or deadline exceeded".into(),
                })
                .await;
            let _ = ctx
                .events
                .send(Event::Done {
                    dir: p.dir,
                    hops: n,
                    delivered: false,
                    elapsed: started.elapsed(),
                })
                .await;
            return;
        }

        // Where are we (exact coords of the current node)?
        let cur_coords: Coords = match ctx.cache.ensure_coords(cur).await {
            Ok(f) => f.value,
            Err(e) => {
                let _ = ctx
                    .events
                    .send(Event::DeadEnd {
                        dir: p.dir,
                        at: cur,
                        coords: None,
                        msg: format!("cannot learn coords: {e:#}"),
                    })
                    .await;
                break;
            }
        };

        let d = tree_dist(&cur_coords, &p.target_coords);
        if d == 0 {
            if cur == p.target {
                emit_verdict(&ctx, &trust, true).await;
                let _ = ctx
                    .events
                    .send(Event::Done {
                        dir: p.dir,
                        hops: n,
                        delivered: true,
                        elapsed: started.elapsed(),
                    })
                    .await;
                return;
            }
            // Coords resolved to us but this is not the target key: the
            // target moved in the tree (its learned path is stale).
            if p.relearn_budget > 0 {
                let _ = ctx
                    .events
                    .send(Event::Warn {
                        dir: Some(p.dir),
                        msg: format!(
                            "target {} appears to have moved (coords collide at {:?}); re-learning",
                            p.target.short(),
                            cur_coords
                        ),
                    })
                    .await;
                match ctx.cache.force_coords(p.target).await {
                    Ok(c) => {
                        let target_coords = c.clone();
                        let ctx = Arc::clone(&ctx);
                        // restart walk with fresh target coords
                        restart_with(ctx, target_coords, n);
                        return;
                    }
                    Err(e) => {
                        let _ = ctx
                            .events
                            .send(Event::DeadEnd {
                                dir: p.dir,
                                at: cur,
                                coords: Some(cur_coords),
                                msg: format!("target re-learn failed: {e:#}"),
                            })
                            .await;
                        break;
                    }
                }
            }
            let _ = ctx
                .events
                .send(Event::DeadEnd {
                    dir: p.dir,
                    at: cur,
                    coords: Some(cur_coords),
                    msg: "coords reached but target key not seen (stale path)".into(),
                })
                .await;
            break;
        }

        // Watermark: each hop must land strictly closer in treespace.
        if d >= watermark {
            let _ = ctx
                .events
                .send(Event::DeadEnd {
                    dir: p.dir,
                    at: cur,
                    coords: Some(cur_coords),
                    msg: format!("not strictly closer (d={d} >= watermark={watermark})"),
                })
                .await;
            break;
        }

        // Peers of the current node.
        let _ = ctx
            .events
            .send(Event::Querying {
                dir: p.dir,
                what: "peers".into(),
                key: cur,
                detail: crate::render::fmt_coords(&cur_coords),
            })
            .await;
        let peers: Vec<Key> = if cur == p.self_key {
            match ctx.admin.get_peers().await {
                Ok(list) => {
                    for e in list.iter().filter(|e| e.up) {
                        if let (Ok(k), Some(ns)) = (Key::from_hex(&e.key), e.latency_ns) {
                            ctx.cache.set_rtt(&k, Duration::from_nanos(ns));
                        }
                    }
                    list.into_iter()
                        .filter(|e| e.up)
                        .filter_map(|e| Key::from_hex(&e.key).ok())
                        .collect()
                }
                Err(e) => {
                    let _ = ctx
                        .events
                        .send(Event::DeadEnd {
                            dir: p.dir,
                            at: cur,
                            coords: Some(cur_coords),
                            msg: format!("local getPeers failed: {e:#}"),
                        })
                        .await;
                    break;
                }
            }
        } else {
            match ctx.cache.ensure_peers_tolerant(cur).await {
                Ok(f) => {
                    if f.stale {
                        let _ = ctx
                            .events
                            .send(Event::Warn {
                                dir: Some(p.dir),
                                msg: format!(
                                    "peers of {} from stale cache (refetch failed)",
                                    cur.short()
                                ),
                            })
                            .await;
                    }
                    f.value.iter().copied().collect()
                }
                Err(e) => {
                    let _ = ctx
                        .events
                        .send(Event::DeadEnd {
                            dir: p.dir,
                            at: cur,
                            coords: Some(cur_coords),
                            msg: format!("cannot fetch peers: {e:#}"),
                        })
                        .await;
                    break;
                }
            }
        };

        // Coordinates of the peers: fire-and-forget probes make the local
        // node learn everyone's path, then getPaths harvests them all.
        // Phase 1 probes only the hinted subset (destination's tree view +
        // our tree keys); the bulk scan runs only if no candidate emerges.
        let unknown = ctx.cache.unknown_coords_keys(&peers);
        let mut skipped_bulk = false;
        if !unknown.is_empty() {
            let (prio, rest): (Vec<Key>, Vec<Key>) = unknown
                .into_iter()
                .partition(|k| ctx.hints.contains(k));
            let eval = |resolved: &[(Key, Coords, u64)]| {
                candidates(&cur_coords, &p.target_coords, resolved).1.is_empty()
            };
            if !prio.is_empty() {
                let _ = ctx
                    .events
                    .send(Event::Querying {
                        dir: p.dir,
                        what: format!(
                            "coords: {}/{} known, probing {} hinted",
                            peers.len() - prio.len() - rest.len(),
                            peers.len(),
                            prio.len()
                        ),
                        key: cur,
                        detail: crate::render::fmt_coords(&cur_coords),
                    })
                    .await;
                ctx.cache.fire_probes(&prio);
                ctx.cache.wait_and_harvest(&prio).await;
            }
            let resolved_now = resolved_peers(ctx.as_ref(), &cur, &peers);
            if !rest.is_empty() && (prio.is_empty() || eval(&resolved_now) || p.exact) {
                let _ = ctx
                    .events
                    .send(Event::Querying {
                        dir: p.dir,
                        what: format!("coords: full scan of {} unhinted peers", rest.len()),
                        key: cur,
                        detail: crate::render::fmt_coords(&cur_coords),
                    })
                    .await;
                ctx.cache.fire_probes(&rest);
                ctx.cache.wait_and_harvest(&rest).await;
            } else if !rest.is_empty() {
                skipped_bulk = true;
                let _ = ctx
                    .events
                    .send(Event::Querying {
                        dir: p.dir,
                        what: format!(
                            "coords: skipping {} unhinted peers (candidate found via hints)",
                            rest.len()
                        ),
                        key: cur,
                        detail: crate::render::fmt_coords(&cur_coords),
                    })
                    .await;
            }
        }
        let resolved: Vec<(Key, Coords, u64)> = resolved_peers(ctx.as_ref(), &cur, &peers);
        // Unknown peers only matter when they could have changed the
        // outcome: dead lookups after a full scan, not a deliberate skip.
        if resolved.len() < peers.len() && !skipped_bulk {
            let _ = ctx
                .events
                .send(Event::Warn {
                    dir: Some(p.dir),
                    msg: format!(
                        "{} of {} peers remain unknown (unreachable or lookups failed)",
                        peers.len() - resolved.len(),
                        peers.len()
                    ),
                })
                .await;
        }

        let (d_self, cands) = candidates(&cur_coords, &p.target_coords, &resolved);
        if cands.is_empty() {
            if p.relearn_budget > 0 {
                let _ = ctx
                    .events
                    .send(Event::Warn {
                        dir: Some(p.dir),
                        msg: format!(
                            "no peer of {} is closer to target; re-learning target coords",
                            cur.short()
                        ),
                    })
                    .await;
                match ctx.cache.force_coords(p.target).await {
                    Ok(c) => {
                        restart_with(Arc::clone(&ctx), c, n);
                        return;
                    }
                    Err(e) => {
                        let _ = ctx
                            .events
                            .send(Event::DeadEnd {
                                dir: p.dir,
                                at: cur,
                                coords: Some(cur_coords),
                                msg: format!("target re-learn failed: {e:#}"),
                            })
                            .await;
                        break;
                    }
                }
            }
            let _ = ctx
                .events
                .send(Event::DeadEnd {
                    dir: p.dir,
                    at: cur,
                    coords: Some(cur_coords),
                    msg: "no peer is closer to the destination (broken path or partition)"
                        .into(),
                })
                .await;
            break;
        }

        let next = match select_next(&cands) {
            Some(c) => c,
            None => {
                let _ = ctx
                    .events
                    .send(Event::DeadEnd {
                        dir: p.dir,
                        at: cur,
                        coords: Some(cur_coords),
                        msg: "candidate selection failed".into(),
                    })
                    .await;
                break;
            }
        };

        n += 1;
        let next_coords = resolved
            .iter()
            .find(|(k, _, _)| *k == next.key)
            .map(|(_, c, _)| c.clone())
            .unwrap_or_default();
        let kind = classify(&cur_coords, &next_coords);
        let node = ctx.cache.get_node(&next.key);
        let unknown_peers = peers.len() - resolved.len();
        let hop_trust = hop_trust(next, &cands, unknown_peers);
        trust.record(hop_trust.0, &hop_trust.1);
        let cached = node
            .as_ref()
            .and_then(|d| d.coords_age_ms(crate::model::now_ms()))
            .map(|age| age > 500)
            .unwrap_or(false);

        if ctx.params.prefetch && next.key != p.self_key {
            prefetch_peers(&ctx, next.key);
        }

        // A single hop that improves treespace distance by more than 1
        // skipped that many expected tree hops: a shortcut.
        let skipped = d_self.saturating_sub(1).saturating_sub(next.dist);
        let _ = ctx
            .events
            .send(Event::Hop {
                dir: p.dir,
                n,
                key: next.key,
                coords: next_coords,
                kind,
                d_before: d_self,
                d_after: next.dist,
                rtt: node.as_ref().and_then(|d| d.rtt),
                stale: false,
                cached,
                trust: hop_trust.0,
                trust_notes: hop_trust.1,
                public_ip: ctx.public.lookup(&next.key).map(str::to_string),
            })
            .await;
        let skipped: usize = skipped as usize;
        if skipped > 0 {
            let _ = ctx
                .events
                .send(Event::ShortcutApplied {
                    dir: p.dir,
                    at_hop: n,
                    skipped,
                    from_d: d_self,
                    to_d: next.dist,
                })
                .await;
        }

        if ctx.params.fetch_nodeinfo && next.key != p.target {
            let admin = ctx.admin.clone();
            let events = ctx.events.clone();
            let k = next.key;
            let dir = p.dir;
            tokio::spawn(async move {
                if let Ok(v) = admin.get_node_info(&k).await {
                    let (bn, bv) = nodeinfo_strings(&v);
                    let _ = events
                        .send(Event::NodeInfoAttached {
                            dir,
                            key: k,
                            build_name: bn,
                            build_version: bv,
                        })
                        .await;
                }
            });
        }

        watermark = d_self;
        cur = next.key;

        if n >= p.max_hops {
            let _ = ctx
                .events
                .send(Event::DeadEnd {
                    dir: p.dir,
                    at: cur,
                    coords: ctx.cache.get_node(&cur).and_then(|d| d.coords),
                    msg: format!("max hops ({}) reached", p.max_hops),
                })
                .await;
            break;
        }
    }

    emit_verdict(&ctx, &trust, false).await;
    let _ = ctx
        .events
        .send(Event::Done {
            dir: p.dir,
            hops: n,
            delivered: false,
            elapsed: started.elapsed(),
        })
        .await;
}

/// Restart the walk with re-learned target coordinates (target moved).
/// Kept non-recursive by double spawning: this helper spawns a tiny task
/// that first awaits the Restarted event (so renderers drop superseded
/// rows) and only then spawns the fresh walk.
fn restart_with(ctx: Arc<WalkCtx>, target_coords: Coords, hops: usize) {
    let dir = ctx.params.dir;
    let events = ctx.events.clone();
    let mut params = WalkParams {
        target_coords,
        ..ctx.params.clone()
    };
    params.max_hops = params.max_hops.saturating_sub(hops);
    params.relearn_budget = params.relearn_budget.saturating_sub(1);
    let ctx2 = Arc::new(WalkCtx {
        params,
        cache: ctx.cache.clone(),
        admin: ctx.admin.clone(),
        events: ctx.events.clone(),
        cancel: ctx.cancel.clone(),
        hints: ctx.hints.clone(),
        public: ctx.public.clone(),
    });
    tokio::spawn(async move {
        let _ = events
            .send(Event::Restarted {
                dir,
                msg: format!(
                    "target coordinates changed; re-tracing (previous {hops} hops superseded)"
                ),
            })
            .await;
        tokio::spawn(walk(ctx2));
    });
}

fn resolved_peers(ctx: &WalkCtx, cur: &Key, peers: &[Key]) -> Vec<(Key, Coords, u64)> {
    peers
        .iter()
        .filter_map(|k| {
            ctx.cache
                .get_node(k)
                .and_then(|d| d.coords.clone())
                .map(|c| (*k, c, link_cost(ctx, cur, k)))
        })
        .collect()
}

fn link_cost(ctx: &WalkCtx, cur: &Key, key: &Key) -> u64 {
    // Our own link costs are only meaningful for decisions made BY our
    // node (hop 1). When a remote node X decides, its cost to the
    // candidate is unknowable and must be the minimum (1 ms): applying OUR
    // latency to a shared peer penalized candidates that merely happen to
    // also peer us.
    if cur == &ctx.params.self_key {
        ctx.params
            .self_link_costs
            .get(key)
            .copied()
            .unwrap_or(1)
            .max(1)
    } else {
        1
    }
}

/// Running per-walk confidence tally.
#[derive(Default)]
struct TrustTally {
    high: usize,
    medium: usize,
    low: usize,
    notes: Vec<String>,
}

impl TrustTally {
    fn record(&mut self, level: TrustLevel, notes: &[String]) {
        match level {
            TrustLevel::High => self.high += 1,
            TrustLevel::Medium => self.medium += 1,
            TrustLevel::Low => self.low += 1,
        }
        for n in notes {
            // Keep the category (text before ':' or '(') for aggregation.
            let cat = n.split([':', '(']).next().unwrap_or(n).trim();
            self.notes.push(cat.to_string());
        }
    }

    fn level(&self) -> TrustLevel {
        if self.low > 0 {
            TrustLevel::Low
        } else if self.medium > 0 {
            TrustLevel::Medium
        } else {
            TrustLevel::High
        }
    }

    fn summarized_notes(&self) -> Vec<String> {
        let mut counts: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();
        for n in &self.notes {
            *counts.entry(n.as_str()).or_default() += 1;
        }
        let mut v: Vec<String> = counts
            .into_iter()
            .map(|(k, c)| if c > 1 { format!("{k} ×{c}") } else { k.to_string() })
            .collect();
        v.sort();
        v
    }
}

async fn emit_verdict(ctx: &WalkCtx, trust: &TrustTally, delivered: bool) {
    let _ = ctx
        .events
        .send(Event::Verdict {
            dir: ctx.params.dir,
            level: trust.level(),
            high: trust.high,
            medium: trust.medium,
            low: trust.low,
            delivered,
            notes: trust.summarized_notes(),
        })
        .await;
}

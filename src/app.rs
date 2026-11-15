//! Orchestration: bootstrap, destination resolution, self-coordinates,
//! speculative route, spawning the forward/reverse walks, cache persistence.

use std::collections::HashMap;
use std::net::{Ipv6Addr, SocketAddrV6, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::admin::{AdminClient, Endpoint};
use crate::events::{Dir, Event, EventRx, EventTx, SpecHop};
use crate::model::{NodeCache, NodeData};
use crate::opts::Opts;
use crate::persist;
use crate::walk::{walk, WalkCtx, WalkParams};
use crate::ygg::{addr_for_key, common_prefix_len, subnet_for_key, subnet_prefix, Coords, Key};

pub struct App {
    pub opts: Opts,
    pub admin: Arc<AdminClient>,
    pub cache: Arc<NodeCache>,
    pub self_key: Key,
    pub self_address: Ipv6Addr,
    /// parent map from the local tree dump (our view of the spanning tree)
    pub parent_of: HashMap<Key, Key>,
    /// link costs (ms) to our own peers
    pub self_link_costs: HashMap<Key, u64>,
    cache_path: Option<std::path::PathBuf>,
    cache_loaded: usize,
}

impl App {
    /// Bootstrap against the admin socket: identity, local tree/paths/peers,
    /// persistent cache. Does not emit events (hard failures are returned).
    pub async fn bootstrap(opts: &Opts) -> Result<Arc<App>> {
        let endpoint = match &opts.endpoint {
            Some(e) => Endpoint::parse(e)?,
            None => match std::env::var("YTRACEROUTE_ENDPOINT") {
                Ok(v) if !v.is_empty() => Endpoint::parse(&v)?,
                _ => Endpoint::detect(),
            },
        };
        let admin = Arc::new(AdminClient::new(
            endpoint,
            Duration::from_secs_f64(opts.timeout),
            opts.parallelism,
        ));

        // Persistent cache first: its keys also help resolve address inputs.
        let cache = NodeCache::new(Duration::from_secs(opts.cache_ttl), admin.clone());
        let cache_path = if opts.no_cache {
            None
        } else {
            opts.cache_file.clone().or_else(persist::default_cache_path)
        };
        let mut cache_loaded = 0usize;
        if let Some(path) = &cache_path {
            match persist::load(&cache, path) {
                Ok(n) => cache_loaded = n,
                Err(e) if path.exists() => {
                    // A corrupt cache must not kill the run.
                    eprintln!("warning: cache load failed: {e:#}");
                }
                Err(_) => {}
            }
        }

        let getself = admin
            .get_self()
            .await
            .context("getSelf failed: is the yggdrasil admin socket reachable?")?;
        let self_key = Key::from_hex(&getself.key)?;
        let self_address: Ipv6Addr = getself
            .address
            .parse()
            .unwrap_or_else(|_| addr_for_key(&self_key));

        let (tree, peers, paths) = tokio::join!(
            admin.get_tree(),
            admin.get_peers(),
            admin.get_paths()
        );
        let parent_of = tree
            .unwrap_or_default()
            .iter()
            .filter_map(|t| {
                Some((
                    Key::from_hex(&t.key).ok()?,
                    Key::from_hex(&t.parent).ok()?,
                ))
            })
            .collect::<HashMap<_, _>>();
        let mut self_link_costs = HashMap::new();
        if let Ok(peers) = peers {
            for p in peers.iter().filter(|p| p.up) {
                if let Ok(k) = Key::from_hex(&p.key) {
                    // cost is already the routing cost (ms, min 1); latency is
                    // a fallback for older remotes that do not expose cost.
                    let cost = if p.cost > 0 {
                        p.cost
                    } else {
                        p.latency_ns.map(|ns| (ns / 1_000_000).max(1)).unwrap_or(1)
                    };
                    self_link_costs.insert(k, cost);
                }
            }
        }
        cache.set_self_info(&self_key, Some(getself.routing_entries));
        if let Ok(paths) = paths {
            cache.harvest_paths(&paths);
        }

        Ok(Arc::new(App {
            opts: opts.clone(),
            admin,
            cache,
            self_key,
            self_address,
            parent_of,
            self_link_costs,
            cache_path,
            cache_loaded,
        }))
    }

    /// Run the trace in a spawned task; returns the join handle and the
    /// event stream (the consumer MUST keep draining events).
    pub fn spawn_run(self: &Arc<Self>) -> (tokio::task::JoinHandle<Result<()>>, EventRx) {
        let (tx, rx) = mpsc::channel(64);
        let app = self.clone();
        let handle = tokio::spawn(async move { app.run(tx).await });
        (handle, rx)
    }

    async fn run(self: &Arc<Self>, events: EventTx) -> Result<()> {
        let cache_path = self.cache_path.clone();
        let cache_loaded = self.cache_loaded;
        let started = Instant::now();
        let cancel = CancellationToken::new();
        let opts = &self.opts;

        // ---- Resolve the destination ------------------------------------
        let dest_key = self
            .resolve_dest(&opts.dest, &events)
            .await
            .context("resolving destination")?;
        if dest_key == self.self_key {
            bail!("destination is this node itself; nothing to trace");
        }

        // ---- Self coordinates (self-probe trick) ------------------------
        let self_coords = self.learn_self_coords(&events).await?;

        // ---- Probe-priority hints ----------------------------------------
        // The destination's own tree view (its ancestry + peers + their
        // ancestries, keys only) plus our tree keys: at every hop these are
        // the most likely next-hop candidates, so they get probed first and
        // the bulk peer scan often never runs.
        let hints: Arc<std::collections::HashSet<Key>> = Arc::new(
            match self.admin.debug_remote_get_tree(&dest_key).await {
                Ok(view) => {
                    let mut set: std::collections::HashSet<Key> = view.into_iter().collect();
                    set.extend(self.parent_of.keys().copied());
                    set.extend(self.parent_of.values().copied());
                    set.extend(self.cache.snapshot().keys());
                    set
                }
                Err(e) => {
                    let _ = events
                        .send(Event::Warn {
                            dir: None,
                            msg: format!(
                                "dest tree view unavailable ({}), falling back to full peer scans",
                                e.root_cause()
                            ),
                        })
                        .await;
                    // Degrade to our tree keys + cache only.
                    let mut set: std::collections::HashSet<Key> = Default::default();
                    set.extend(self.parent_of.keys().copied());
                    set.extend(self.parent_of.values().copied());
                    set.extend(self.cache.snapshot().keys());
                    set
                }
            },
        );

        // ---- Destination coordinates + reachability ----------------------
        let dest_f = self
            .cache
            .ensure_coords(dest_key)
            .await
            .with_context(|| format!("probing destination {}", dest_key.short()))?;
        let dest_coords = dest_f.value;
        let dest_rtt = self.cache.get_node(&dest_key).and_then(|d| d.rtt);

        // Speculative tree routes for both directions (displayed dimmed).
        for dir in [Dir::Forward, Dir::Reverse] {
            let route = self.speculative_route(dir, &self_coords, &dest_coords);
            let _ = events.send(Event::SpecRoute { dir, route }).await;
        }

        let dest_address = addr_for_key(&dest_key);
        let _ = events
            .send(Event::Started {
                dest_input: opts.dest.clone(),
                self_key: self.self_key,
                self_address: self.self_address,
                self_coords: self_coords.clone(),
                dest_key,
                dest_address,
                dest_coords: dest_coords.clone(),
                dest_rtt,
                dest_stale: dest_f.stale,
                cache_loaded,
            })
            .await;

        // ---- Walks -------------------------------------------------------
        let deadline = started + Duration::from_secs(opts.deadline);
        let mut handles = Vec::new();
        {
            let ctx = Arc::new(WalkCtx {
                params: WalkParams {
                    dir: Dir::Forward,
                    self_key: self.self_key,
                    start: self.self_key,
                    target: dest_key,
                    target_coords: dest_coords.clone(),
                    max_hops: opts.max_hops,
                    deadline,
                    self_link_costs: self.self_link_costs.clone(),
                    prefetch: !opts.no_prefetch,
                    fetch_nodeinfo: opts.nodeinfo,
                    exact: opts.exact,
                    relearn_budget: 1,
                },
                cache: self.cache.clone(),
                admin: self.admin.clone(),
                events: events.clone(),
                cancel: cancel.clone(),
                hints: hints.clone(),
            });
            handles.push(tokio::spawn(walk(ctx)));
        }
        if !opts.no_reverse {
            // Pre-fetch the destination's peers so the reverse walk starts
            // instantly after the forward one finishes probing it.
            if !opts.no_prefetch {
                let cache = self.cache.clone();
                let k = dest_key;
                tokio::spawn(async move {
                    let _ = cache.ensure_peers(k).await;
                });
            }
            let ctx = Arc::new(WalkCtx {
                params: WalkParams {
                    dir: Dir::Reverse,
                    self_key: self.self_key,
                    start: dest_key,
                    target: self.self_key,
                    target_coords: self_coords.clone(),
                    max_hops: opts.max_hops,
                    deadline,
                    self_link_costs: self.self_link_costs.clone(),
                    prefetch: !opts.no_prefetch,
                    fetch_nodeinfo: opts.nodeinfo,
                    exact: opts.exact,
                    relearn_budget: 1,
                },
                cache: self.cache.clone(),
                admin: self.admin.clone(),
                events: events.clone(),
                cancel: cancel.clone(),
                hints: hints.clone(),
            });
            handles.push(tokio::spawn(walk(ctx)));
        }

        // Ctrl-C cancels both walks.
        {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                cancel.cancel();
            });
        }

        for h in handles {
            let _ = h.await;
        }

        // ---- Final validation + persistence ------------------------------
        match self.cache.force_coords(dest_key).await {
            Ok(c) if c != dest_coords => {
                let _ = events
                    .send(Event::Warn {
                        dir: None,
                        msg: format!(
                            "destination moved during trace: {:?} -> {c:?}",
                            dest_coords
                        ),
                    })
                    .await;
            }
            Err(e) => {
                let _ = events
                    .send(Event::Warn {
                        dir: None,
                        msg: format!("final destination re-probe failed: {e:#}"),
                    })
                    .await;
            }
            _ => {}
        }

        let saved_nodes = if let Some(path) = &cache_path {
            match persist::save(&self.cache, Some(self.self_key), path) {
                Ok(n) => n,
                Err(e) => {
                    let _ = events
                        .send(Event::Warn {
                            dir: None,
                            msg: format!("cache save failed: {e:#}"),
                        })
                        .await;
                    0
                }
            }
        } else {
            0
        };

        let _ = events
            .send(Event::Finished {
                elapsed: started.elapsed(),
                saved_nodes,
                cache_path: cache_path
                    .as_ref()
                    .map(|p| p.display().to_string()),
            })
            .await;
        Ok(())
    }

    /// Resolve the destination input: hex key, "self", or an IPv6 address
    /// (node `02..` or routed subnet `03..`). Address inputs are matched
    /// against every key we know (tree/paths/peers/cache); if that fails we
    /// send a UDP packet through the overlay (the "ping6 precheck") and
    /// match the session that appears in getSessions.
    async fn resolve_dest(&self, input: &str, events: &EventTx) -> Result<Key> {
        let t = input.trim();
        if t.eq_ignore_ascii_case("self") || t.eq_ignore_ascii_case("me") {
            return Ok(self.self_key);
        }
        if let Ok(k) = Key::from_hex(t) {
            return Ok(k);
        }
        let addr: Ipv6Addr = t
            .parse()
            .with_context(|| format!("destination {t:?} is neither a hex key nor an IPv6 address"))?;
        let octets = addr.octets();
        let is_subnet = (octets[0] & 0xfe) == 0x02 && (octets[0] & 0x01) == 1;

        if let Some(k) = self.match_known_keys(&addr, is_subnet) {
            return Ok(k);
        }

        // Fall back to a UDP probe through the overlay: any packet to the
        // address makes the local node resolve the owner and open a session,
        // which getSessions then reports with the key.
        let _ = events
            .send(Event::Warn {
                dir: None,
                msg: format!(
                    "address {addr} not in local tables; probing via overlay (UDP + getSessions)"
                ),
            })
            .await;
        self.probe_and_match(addr, is_subnet).await
    }

    fn match_known_keys(&self, addr: &Ipv6Addr, is_subnet: bool) -> Option<Key> {
        let mut candidates: Vec<Key> = self.parent_of.keys().copied().collect();
        for (k, d) in self.cache.snapshot() {
            let _ = d;
            candidates.push(k);
        }
        // also peers
        if let Some(NodeData { peers: Some(p), .. }) = self.cache.get_node(&self.self_key) {
            candidates.extend(p.iter().copied());
        }
        candidates.dedup();
        candidates.into_iter().find(|k| {
            if is_subnet {
                subnet_for_key(k) == subnet_prefix(addr)
            } else {
                &addr_for_key(k) == addr
            }
        })
    }

    async fn probe_and_match(&self, addr: Ipv6Addr, is_subnet: bool) -> Result<Key> {
        let sock = UdpSocket::bind("[::]:0").context("binding UDP socket for overlay probe")?;
        let probe = SocketAddrV6::new(addr, 9, 0, 0); // port 9: discard
        sock.send_to(b"ytraceroute-probe", probe)
            .context("sending UDP probe through the overlay (is the yggdrasil TUN up?)")?;

        let deadline = Instant::now() + Duration::from_secs(6);
        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let sessions = self.admin.get_sessions().await?;
            for s in sessions {
                if let Ok(k) = Key::from_hex(&s.key) {
                    let m = if is_subnet {
                        subnet_for_key(&k) == subnet_prefix(&addr)
                    } else {
                        addr_for_key(&k) == addr
                    };
                    if m {
                        return Ok(k);
                    }
                }
            }
        }
        bail!(
            "could not resolve {addr}: no session appeared; node unreachable or address unknown"
        )
    }

    /// Learn our own coordinates. `debug_remoteGetSelf` addressed to our own
    /// key makes the node run a path lookup for itself; the answer shows up
    /// in `getPaths` (the RPC itself legitimately times out).
    async fn learn_self_coords(&self, _events: &EventTx) -> Result<Coords> {
        if let Some(d) = self.cache.get_node(&self.self_key)
            && let Some(c) = d.coords {
                return Ok(c);
            }
        // The probe response will never arrive; use a short client-side
        // timeout and poll getPaths instead.
        let _ = self
            .admin
            .request_with_timeout(
                "debug_remoteGetSelf",
                serde_json::json!({ "key": self.self_key.hex() }),
                Duration::from_millis(1200),
            )
            .await;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            let paths = self.admin.get_paths().await?;
            self.cache.harvest_paths(&paths);
            if let Some(d) = self.cache.get_node(&self.self_key)
                && let Some(c) = d.coords {
                    return Ok(c);
                }
            tokio::time::sleep(Duration::from_millis(350)).await;
        }
        Err(anyhow!(
            "could not learn self coordinates (is the node connected to the network?)"
        ))
    }

    /// Speculative "via root" route for a direction: climb from the source
    /// to the branch point (LCA), then descend the destination's coordinate
    /// prefixes. Ancestor keys come from the local tree dump; descending
    /// nodes are coords-only until confirmed (or matched in cache).
    fn speculative_route(
        &self,
        dir: Dir,
        self_coords: &Coords,
        dest_coords: &Coords,
    ) -> Vec<SpecHop> {
        let (src_coords, dst_coords, ancestors) = match dir {
            Dir::Forward => (self_coords, dest_coords, self.ancestor_keys(self.self_key)),
            Dir::Reverse => (dest_coords, self_coords, Vec::new()),
        };
        let lcp = common_prefix_len(src_coords, dst_coords);
        let mut route = Vec::new();

        // Climb: nodes at prefix depths len-1 ..= lcp of the source path.
        for depth in (lcp..src_coords.len()).rev() {
            let coords = src_coords[..depth].to_vec();
            let key = ancestors.get(depth).copied().flatten();
            route.push(SpecHop { key, coords });
        }

        // Descend: prefix depths lcp+1 ..= len of the destination path.
        for depth in (lcp + 1)..=dst_coords.len() {
            let coords = dst_coords[..depth].to_vec();
            let key = self.key_with_coords(&coords);
            route.push(SpecHop { key, coords });
        }
        route
    }

    /// Our ancestry from the root (index = depth, so ancestors[depth] is the
    /// key of the node whose coords are self_coords[..depth]).
    fn ancestor_keys(&self, mut key: Key) -> Vec<Option<Key>> {
        let mut chain = vec![Some(key)];
        let mut guard = 0;
        while let Some(parent) = self.parent_of.get(&key).copied() {
            if parent == key || guard > 128 {
                break;
            }
            chain.push(Some(parent));
            key = parent;
            guard += 1;
        }
        chain.reverse(); // root first
        chain
    }

    /// If any known node has exactly these coords, return its key.
    fn key_with_coords(&self, coords: &Coords) -> Option<Key> {
        self.cache
            .snapshot()
            .into_iter()
            .find(|(_, d)| d.coords.as_deref() == Some(coords.as_slice()))
            .map(|(k, _)| k)
    }
}

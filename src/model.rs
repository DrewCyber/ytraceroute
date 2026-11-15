//! Shared node knowledge base for both walk directions.
//!
//! Per key we cache learned coordinates (from `getPaths` after probing the
//! node) and its peer list (from `debug_remoteGetPeers`), with epoch-ms
//! timestamps so entries can expire (TTL) while still serving as stale
//! fallbacks. Concurrent fetches of the same key are deduplicated through
//! shared futures.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{anyhow, Result};
use futures::future::{BoxFuture, FutureExt, Shared};
use tokio::sync::Mutex;

use crate::admin::{AdminClient, PathEntry};
use crate::ygg::{Coords, Key};

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
pub struct NodeData {
    pub coords: Option<Coords>,
    pub coords_at_ms: Option<u64>,
    pub peers: Option<Arc<Vec<Key>>>,
    pub peers_at_ms: Option<u64>,
    pub rtt: Option<Duration>,
    pub routing_entries: Option<u64>,
    pub last_seen_ms: u64,
}

impl NodeData {
    fn new() -> Self {
        NodeData {
            coords: None,
            coords_at_ms: None,
            peers: None,
            peers_at_ms: None,
            rtt: None,
            routing_entries: None,
            last_seen_ms: now_ms(),
        }
    }
    pub fn coords_age_ms(&self, now: u64) -> Option<u64> {
        self.coords_at_ms.map(|t| now.saturating_sub(t))
    }
    pub fn peers_age_ms(&self, now: u64) -> Option<u64> {
        self.peers_at_ms.map(|t| now.saturating_sub(t))
    }
}

#[derive(Debug, Clone)]
pub struct Fetched<T> {
    pub value: T,
    pub stale: bool,
    #[allow(dead_code)]
    pub age_ms: Option<u64>,
}

type AnyErr = Arc<anyhow::Error>;
type CoordsOut = Result<Coords, AnyErr>;
type PeersOut = Result<Arc<Vec<Key>>, AnyErr>;
type SharedCoords = Shared<BoxFuture<'static, CoordsOut>>;
type SharedPeers = Shared<BoxFuture<'static, PeersOut>>;

pub struct NodeCache {
    ttl_ms: u64,
    nodes: RwLock<HashMap<Key, NodeData>>,
    inflight_coords: Mutex<HashMap<Key, SharedCoords>>,
    inflight_peers: Mutex<HashMap<Key, SharedPeers>>,
    admin: Arc<AdminClient>,
}

impl NodeCache {
    pub fn new(ttl: Duration, admin: Arc<AdminClient>) -> Arc<Self> {
        Arc::new(NodeCache {
            ttl_ms: ttl.as_millis() as u64,
            nodes: RwLock::new(HashMap::new()),
            inflight_coords: Mutex::new(HashMap::new()),
            inflight_peers: Mutex::new(HashMap::new()),
            admin,
        })
    }

    fn with_node<T>(&self, key: &Key, f: impl FnOnce(&mut NodeData) -> T) -> T {
        let mut nodes = self.nodes.write().expect("node cache poisoned");
        let e = nodes.entry(*key).or_insert_with(NodeData::new);
        e.last_seen_ms = now_ms();
        f(e)
    }

    pub fn get_node(&self, key: &Key) -> Option<NodeData> {
        self.nodes.read().ok()?.get(key).cloned()
    }

    pub fn snapshot(&self) -> HashMap<Key, NodeData> {
        self.nodes.read().map(|n| n.clone()).unwrap_or_default()
    }

    /// Seed from a previous run's persisted state.
    pub fn seed(&self, key: Key, data: NodeData) {
        let mut nodes = self.nodes.write().expect("node cache poisoned");
        nodes.entry(key).or_insert(data);
    }

    /// Merge freshly learned `getPaths` entries: signed coordinates claimed
    /// by the destination nodes themselves (highest-truth source).
    pub fn harvest_paths(&self, paths: &[PathEntry]) -> usize {
        let now = now_ms();
        let mut n = 0;
        for p in paths {
            if let Ok(k) = Key::from_hex(&p.key) {
                self.with_node(&k, |e| {
                    let changed = e.coords.as_deref() != Some(p.path.as_slice());
                    e.coords = Some(p.path.clone());
                    e.coords_at_ms = Some(now);
                    if changed {
                        n += 1;
                    }
                });
            }
        }
        n
    }

    pub fn set_self_info(&self, key: &Key, routing_entries: Option<u64>) {
        self.with_node(key, |e| {
            e.routing_entries = routing_entries;
            e.rtt = Some(Duration::ZERO);
        });
    }

    pub fn set_rtt(&self, key: &Key, rtt: Duration) {
        self.with_node(key, |e| e.rtt = Some(rtt));
    }

    fn coords_fresh(&self, key: &Key) -> Option<Fetched<Coords>> {
        let now = now_ms();
        let nodes = self.nodes.read().ok()?;
        let e = nodes.get(key)?;
        let coords = e.coords.clone()?;
        let age = e.coords_age_ms(now);
        Some(Fetched {
            value: coords,
            stale: age.map(|a| a > self.ttl_ms).unwrap_or(true),
            age_ms: age,
        })
    }

    fn peers_fresh(&self, key: &Key) -> Option<Fetched<Arc<Vec<Key>>>> {
        let now = now_ms();
        let nodes = self.nodes.read().ok()?;
        let e = nodes.get(key)?;
        let peers = e.peers.clone()?;
        let age = e.peers_age_ms(now);
        Some(Fetched {
            value: peers,
            stale: age.map(|a| a > self.ttl_ms).unwrap_or(true),
            age_ms: age,
        })
    }

    /// Ensure we know the coordinates of `key`, probing it over the overlay
    /// if needed (the probe both proves reachability and makes the local
    /// node learn the path, which then shows up in `getPaths`).
    pub async fn ensure_coords(self: &Arc<Self>, key: Key) -> Result<Fetched<Coords>> {
        if let Some(f) = self.coords_fresh(&key)
            && !f.stale {
                return Ok(f);
            }
        let shared = {
            let mut inflight = self.inflight_coords.lock().await;
            inflight
                .entry(key)
                .or_insert_with(|| {
                    let cache = self.clone();
                    async move {
                        let out = cache.fetch_coords(key).await.map_err(Arc::new);
                        cache.inflight_coords.lock().await.remove(&key);
                        out
                    }
                    .boxed()
                    .shared()
                })
                .clone()
        };
        let coords = match shared.await {
            Ok(c) => c,
            Err(e) => {
                // A dead probe is not fatal if we still have (stale) coords:
                // the walk can proceed while flagging the uncertainty.
                if let Some(f) = self.coords_fresh(&key) {
                    return Ok(f);
                }
                return Err(anyhow!("coords for {}: {e}", key.short()));
            }
        };
        Ok(Fetched {
            value: coords,
            stale: false,
            age_ms: Some(0),
        })
    }

    /// Force a re-probe even if a fresh value exists (used to re-validate
    /// the destination after a dead end or at completion).
    pub async fn force_coords(self: &Arc<Self>, key: Key) -> Result<Coords> {
        self.inflight_coords.lock().await.remove(&key);
        self.with_node(&key, |e| e.coords_at_ms = None);
        self.ensure_coords(key).await.map(|f| f.value)
    }

    async fn fetch_coords(&self, key: Key) -> Result<Coords> {
        let r = self.admin.debug_remote_get_self(&key).await?;
        let now = now_ms();
        self.with_node(&key, |e| {
            e.rtt = Some(r.rtt);
            e.routing_entries = r.routing_entries;
            e.last_seen_ms = now;
        });
        // The path should already be learned by the time the probe response
        // arrives; poll a couple of times just in case.
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            let paths = self.admin.get_paths().await?;
            self.harvest_paths(&paths);
            if let Some(c) = self.coords_fresh(&key).map(|f| f.value) {
                return Ok(c);
            }
        }
        anyhow::bail!("probe answered but no path learned for {}", key.short())
    }

    /// Ensure we know the peer list of a remote `key`.
    pub async fn ensure_peers(self: &Arc<Self>, key: Key) -> Result<Fetched<Arc<Vec<Key>>>> {
        if let Some(f) = self.peers_fresh(&key)
            && !f.stale {
                return Ok(f);
            }
        let shared = {
            let mut inflight = self.inflight_peers.lock().await;
            let (f, inserted) = match inflight.get(&key) {
                Some(existing) => (existing.clone(), false),
                None => {
                    let f: SharedPeers = {
                        let cache = self.clone();
                        async move {
                            let out = cache
                                .fetch_peers(key)
                                .await
                                .map_err(Arc::new);
                            cache.inflight_peers.lock().await.remove(&key);
                            out
                        }
                        .boxed()
                        .shared()
                    };
                    inflight.insert(key, f.clone());
                    (f, true)
                }
            };
            let _ = inserted;
            f
        };
        let peers = shared
            .await
            .map_err(|e| anyhow!("peers of {}: {e}", key.short()))?;
        // Fall back to a stale list if the refetch is impossible.
        Ok(Fetched {
            value: peers,
            stale: false,
            age_ms: Some(0),
        })
    }

    /// Best-effort variant: if the fetch fails but stale data exists, return
    /// the stale data with `stale: true` instead of an error.
    pub async fn ensure_peers_tolerant(
        self: &Arc<Self>,
        key: Key,
    ) -> Result<Fetched<Arc<Vec<Key>>>> {
        match self.ensure_peers(key).await {
            Ok(f) => Ok(f),
            Err(e) => {
                if let Some(f) = self.peers_fresh(&key) {
                    Ok(f)
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn fetch_peers(&self, key: Key) -> Result<Arc<Vec<Key>>> {
        let peers = self.admin.debug_remote_get_peers(&key).await?;
        let now = now_ms();
        let peers = Arc::new(peers);
        self.with_node(&key, |e| {
            e.peers = Some(peers.clone());
            e.peers_at_ms = Some(now);
        });
        Ok(peers)
    }

    /// Keys from `keys` we have no fresh coordinates for.
    pub fn unknown_coords_keys(&self, keys: &[Key]) -> Vec<Key> {
        let now = now_ms();
        let nodes = self.nodes.read().expect("node cache poisoned");
        keys.iter()
            .filter(|k| {
                nodes
                    .get(*k)
                    .and_then(|e| e.coords_at_ms)
                    .map(|t| now.saturating_sub(t) > self.ttl_ms)
                    .unwrap_or(true)
            })
            .copied()
            .collect()
    }

    /// Fire-and-forget probes (paced chunks); the lookup side effect on the
    /// local node is what matters, responses are abandoned.
    pub fn fire_probes(&self, keys: &[Key]) {
        const CHUNK: usize = 64;
        let keys = keys.to_vec();
        let admin = self.admin.clone();
        std::thread::spawn(move || {
            for chunk in keys.chunks(CHUNK) {
                for k in chunk {
                    admin.fire(
                        "debug_remoteGetSelf",
                        serde_json::json!({ "key": k.hex() }),
                        Duration::from_millis(1500),
                    );
                }
                // Pacing between chunks to stay polite to the admin socket
                // and the overlay; overlaps with the caller's harvest wait.
                std::thread::sleep(Duration::from_millis(120));
            }
        });
    }

    /// Give the triggered lookups time to land, then harvest `getPaths`
    /// (with retries). Returns how many of `keys` became known.
    pub async fn wait_and_harvest(&self, keys: &[Key]) -> usize {
        let want: std::collections::HashSet<Key> = keys.iter().copied().collect();
        let mut learned = 0;
        for round in 0..3 {
            tokio::time::sleep(Duration::from_millis(if round == 0 {
                2500
            } else {
                1200
            }))
            .await;
            if let Ok(paths) = self.admin.get_paths().await {
                self.harvest_paths(&paths);
            }
            learned = want.iter().filter(|k| self.coords_fresh(k).is_some()).count();
            if learned == want.len() {
                break;
            }
        }
        learned
    }
}
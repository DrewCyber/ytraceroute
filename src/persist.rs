//! Persistent on-disk node cache so that repeated or similar traces start
//! from what was learned last run.
//!
//! JSON at $XDG_CACHE_HOME/ytraceroute/cache.json (fallback ~/.cache/...),
//! written atomically (tmp + rename), LRU-trimmed by last_seen. Entries keep
//! epoch-ms timestamps; freshness is judged against --cache-ttl at use time,
//! so an old file degrades gracefully into stale hints.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

use crate::model::{NodeData, NodeCache};
use crate::ygg::Key;

const MAX_NODES: usize = 10_000;

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistNode {
    coords: Option<Vec<u64>>,
    coords_at_ms: Option<u64>,
    peers: Option<Vec<String>>,
    peers_at_ms: Option<u64>,
    rtt_ms: Option<u64>,
    routing_entries: Option<u64>,
    last_seen_ms: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistFile {
    version: u32,
    saved_at_ms: u64,
    self_key: Option<String>,
    nodes: HashMap<String, PersistNode>,
}

pub fn default_cache_path() -> Option<PathBuf> {
    dirs::cache_dir().map(|d| d.join("ytraceroute").join("cache.json"))
}

pub fn load(cache: &NodeCache, path: &Path) -> Result<usize> {
    let raw = std::fs::read(path)
        .with_context(|| format!("reading cache file {}", path.display()))?;
    let file: PersistFile = serde_json::from_slice(&raw)
        .with_context(|| format!("parsing cache file {}", path.display()))?;
    let mut n = 0;
    for (khex, p) in file.nodes {
        if let Ok(key) = Key::from_hex(&khex) {
            cache.seed(
                key,
                NodeData {
                    coords: p.coords,
                    coords_at_ms: p.coords_at_ms,
                    peers: p.peers.map(|v| std::sync::Arc::new(
                        v.iter().filter_map(|s| Key::from_hex(s).ok()).collect(),
                    )),
                    peers_at_ms: p.peers_at_ms,
                    rtt: p.rtt_ms.map(Duration::from_millis),
                    routing_entries: p.routing_entries,
                    last_seen_ms: p.last_seen_ms,
                },
            );
            n += 1;
        }
    }
    Ok(n)
}

pub fn save(cache: &NodeCache, self_key: Option<Key>, path: &Path) -> Result<usize> {
    let snapshot = cache.snapshot();
    let mut nodes: HashMap<String, PersistNode> = HashMap::new();
    for (k, d) in &snapshot {
        // Skip nodes we learned nothing durable about.
        if d.coords.is_none() && d.peers.is_none() {
            continue;
        }
        nodes.insert(
            k.hex(),
            PersistNode {
                coords: d.coords.clone(),
                coords_at_ms: d.coords_at_ms,
                peers: d
                    .peers
                    .as_ref()
                    .map(|p| p.iter().map(|k| k.hex()).collect()),
                peers_at_ms: d.peers_at_ms,
                rtt_ms: d.rtt.map(|r| r.as_millis() as u64),
                routing_entries: d.routing_entries,
                last_seen_ms: d.last_seen_ms,
            },
        );
    }
    // LRU trim.
    if nodes.len() > MAX_NODES {
        let mut entries: Vec<_> = nodes.into_iter().collect();
        entries.sort_by_key(|(_, p)| std::cmp::Reverse(p.last_seen_ms));
        entries.truncate(MAX_NODES);
        nodes = entries.into_iter().collect();
    }
    let file = PersistFile {
        version: 1,
        saved_at_ms: crate::model::now_ms(),
        self_key: self_key.map(|k| k.hex()),
        nodes,
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let raw = serde_json::to_vec(&file)?;
    std::fs::write(&tmp, raw).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming into {}", path.display()))?;
    Ok(file.nodes.len())
}

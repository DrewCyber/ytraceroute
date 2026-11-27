//! Public peer discovery: fetch the community-maintained public nodes list
//! and label trace hops that hit a public node with its public endpoint.
//!
//! Sources (tried in order; both are mirrors of the same aggregation):
//! https://publicpeers.neilalexander.dev/publicnodes.json (primary),
//! https://peers.yggdrasil.link/publicnodes.json (backup). The parsed
//! key→endpoint map is cached on disk next to the node cache for 1 hour.
//! Every failure is silent: no list means no labels, never a warning.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::ygg::Key;

const SOURCES: [&str; 2] = [
    "https://publicpeers.neilalexander.dev/publicnodes.json",
    "https://peers.yggdrasil.link/publicnodes.json",
];

const FETCH_TIMEOUT: Duration = Duration::from_secs(4);
/// Hard wall-clock cap for one fetch attempt. ureq's own timeouts do not
/// cover a stalled TCP connect, so cap the whole attempt from the outside
/// and abandon it if needed.
const ATTEMPT_BUDGET: Duration = Duration::from_secs(8);
const CACHE_TTL: Duration = Duration::from_secs(3600);
pub const CACHE_FILE_NAME: &str = "ytraceroute_peers.json";

/// Node key → public endpoint host (an IP or a domain, as written in the
/// peer URI).
#[derive(Default)]
pub struct PublicPeers {
    endpoints: HashMap<Key, String>,
}

impl PublicPeers {
    pub fn lookup(&self, key: &Key) -> Option<&str> {
        self.endpoints.get(key).map(String::as_str)
    }
}

/// The list cache lives next to the node cache file (same directory).
/// `None` (--no-cache): still fetched, just not kept across runs.
pub fn cache_path(node_cache: Option<&Path>) -> Option<PathBuf> {
    node_cache.map(|p| p.with_file_name(CACHE_FILE_NAME))
}

/// Best-effort acquisition: a fresh enough disk cache wins, otherwise the
/// sources are fetched (primary first, then backup). All failures are
/// silent; an empty map simply means no labels.
pub async fn acquire(disk: Option<&Path>) -> Arc<PublicPeers> {
    if let Some(path) = disk
        && let Some((endpoints, at_ms)) = load_disk(path)
        && crate::model::now_ms().saturating_sub(at_ms) < CACHE_TTL.as_millis() as u64
    {
        return Arc::new(PublicPeers { endpoints });
    }
    if let Some((endpoints, at_ms)) = fetch_list().await {
        if let Some(path) = disk {
            save_disk(path, &endpoints, at_ms);
        }
        return Arc::new(PublicPeers { endpoints });
    }
    Arc::new(PublicPeers::default())
}

async fn fetch_list() -> Option<(HashMap<Key, String>, u64)> {
    for url in SOURCES {
        let url = url.to_string();
        let attempt = tokio::time::timeout(
            ATTEMPT_BUDGET,
            tokio::task::spawn_blocking(move || fetch_one(&url)),
        )
        .await
        .ok()? // budget blown -> try the backup
        .ok() // join error / panic
        .and_then(|inner| inner); // transport or parse failure
        if let Some(map) = attempt {
            return Some((map, crate::model::now_ms()));
        }
        // Primary failed or returned nothing: fall through to the backup.
    }
    None
}

fn fetch_one(url: &str) -> Option<HashMap<Key, String>> {
    let agent = ureq::AgentBuilder::new()
        // getaddrinfo lists IPv6 first; on hosts without a working v6 route
        // each stalled connect must give up quickly so the attempt reaches
        // the IPv4 addresses instead of hanging until the OS gives up.
        .timeout_connect(Duration::from_secs(2))
        .timeout(FETCH_TIMEOUT)
        .build();
    let body = agent.get(url).call().ok()?.into_string().ok()?;
    Some(parse(&body))
}

/// Wire shape: { "<country>.md": { "<uri>": { "up": bool, "key": "<hex>",
/// ... } } }. Entries marked down or without a usable key/host are skipped.
fn parse(body: &str) -> HashMap<Key, String> {
    let mut out = HashMap::new();
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return out;
    };
    let Some(top) = v.as_object() else { return out };
    for group in top.values() {
        let Some(group) = group.as_object() else { continue };
        for (uri, info) in group {
            if info.get("up").and_then(serde_json::Value::as_bool) == Some(false) {
                continue;
            }
            let Some(key_hex) = info.get("key").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let Ok(key) = Key::from_hex(key_hex) else { continue };
            let Some(host) = endpoint_host(uri) else { continue };
            // The list may carry several URIs per node (tcp/tls/quic, by IP
            // and by domain name): prefer a literal IP over a domain.
            let prefer = match out.get(&key) {
                None => true,
                Some(old) => !is_ip_literal(old) && is_ip_literal(&host),
            };
            if prefer {
                out.insert(key, host);
            }
        }
    }
    out
}

fn is_ip_literal(s: &str) -> bool {
    s.parse::<std::net::IpAddr>().is_ok()
}

/// Public endpoint host from a peer URI, stripped of everything but the
/// host: "tls://[200:..]:443" → "200:..", "tcp://1.2.3.4:9000" → "1.2.3.4",
/// "tls://name.example:443" → the domain, "tls://host:1339?key=<hex>" →
/// the domain (no port, no query).
fn endpoint_host(uri: &str) -> Option<String> {
    let rest = uri.split_once("://")?.1;
    // Query and fragment may follow the port directly (no path slash).
    let rest = rest.split(['?', '#']).next().unwrap_or(rest);
    let rest = rest.split('/').next().filter(|s| !s.is_empty())?;
    if let Some(inner) = rest.strip_prefix('[') {
        let inner = inner.split(']').next().unwrap_or(inner);
        return if inner.is_empty() { None } else { Some(inner.to_string()) };
    }
    // Strip a trailing :port only when it really looks like one (so bare
    // unbracketed IPv6 keeps its colons).
    match rest.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') && port.parse::<u16>().is_ok() => {
            Some(host.to_string())
        }
        _ => Some(rest.to_string()),
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CacheFile {
    version: u32,
    fetched_at_ms: u64,
    /// key hex → public endpoint host
    endpoints: HashMap<String, String>,
}

fn load_disk(path: &Path) -> Option<(HashMap<Key, String>, u64)> {
    let raw = std::fs::read(path).ok()?;
    let file: CacheFile = serde_json::from_slice(&raw).ok()?;
    let mut endpoints = HashMap::new();
    for (hex, host) in file.endpoints {
        if let Ok(k) = Key::from_hex(&hex) {
            endpoints.insert(k, host);
        }
    }
    Some((endpoints, file.fetched_at_ms))
}

fn save_disk(path: &Path, endpoints: &HashMap<Key, String>, at_ms: u64) {
    let file = CacheFile {
        version: 1,
        fetched_at_ms: at_ms,
        endpoints: endpoints
            .iter()
            .map(|(k, h)| (k.hex(), h.clone()))
            .collect(),
    };
    let Ok(raw) = serde_json::to_vec(&file) else { return };
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, raw).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    } else {
        let _ = std::fs::remove_file(&tmp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
      "armenia.md": {
        "tcp://37.186.113.100:1514": {
          "up": true,
          "key": "2a038bfcc812ef82f0e4a936ccc065553b5beab0e566e2c48830eede873fdcc3"
        },
        "tls://ygg-evn-1.wgos.org:1515": {
          "up": true,
          "key": "2a038bfcc812ef82f0e4a936ccc065553b5beab0e566e2c48830eede873fdcc3"
        },
        "tls://down.example.net:443": {
          "up": false,
          "key": "1111111111111111111111111111111111111111111111111111111111111111"
        },
        "quic://no-key.example.net:443": { "up": true }
      },
      "broken": "not-an-object"
    }"#;

    #[test]
    fn parses_groups_and_skips_down_and_keyless() {
        let m = parse(SAMPLE);
        let up = Key::from_hex("2a038bfcc812ef82f0e4a936ccc065553b5beab0e566e2c48830eede873fdcc3")
            .unwrap();
        // Several URIs per key: the literal IP wins over the domain.
        assert_eq!(m.get(&up).map(String::as_str), Some("37.186.113.100"));
        let down = Key::from_hex("1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
        assert!(!m.contains_key(&down));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn endpoint_hosts() {
        assert_eq!(endpoint_host("tls://[200:1:2::3]:443").as_deref(), Some("200:1:2::3"));
        assert_eq!(endpoint_host("tcp://1.2.3.4:9000").as_deref(), Some("1.2.3.4"));
        assert_eq!(endpoint_host("quic://37.186.113.100:1515").as_deref(), Some("37.186.113.100"));
        assert_eq!(endpoint_host("tls://peer.example.com:443").as_deref(), Some("peer.example.com"));
        assert_eq!(endpoint_host("tcp://no-port-host").as_deref(), Some("no-port-host"));
        // Query string with an embedded key right after the port.
        assert_eq!(
            endpoint_host("tls://ygg2.mk16.de:1339?key=000000d80a2d7b31").as_deref(),
            Some("ygg2.mk16.de")
        );
        assert_eq!(endpoint_host("tls://[200:1:2::3]:443?k=v").as_deref(), Some("200:1:2::3"));
        assert_eq!(endpoint_host("tls://host.example/path?x=1").as_deref(), Some("host.example"));
        assert_eq!(endpoint_host("garbage"), None);
        assert_eq!(endpoint_host("tcp://"), None);
    }

    #[test]
    fn cache_path_is_sibling_of_node_cache() {
        let p = cache_path(Some(Path::new("/tmp/ytraceroute_cache.json"))).unwrap();
        assert_eq!(p, Path::new("/tmp/ytraceroute_peers.json"));
        assert_eq!(cache_path(None), None);
    }
}

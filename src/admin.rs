//! Client for the yggdrasil-go admin socket (v0.5.x).
//!
//! Wire protocol: one JSON request / one JSON response per connection
//! (newline-terminated request; the response envelope is pretty-printed
//! across multiple lines, so we decode a single JSON value from the stream
//! rather than reading line-by-line). See yggdrasil-go src/admin/admin.go.

use std::io::{Read, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::ygg::Key;

#[derive(Clone, Debug)]
pub enum Endpoint {
    Unix(PathBuf),
    Tcp(String),
}

impl Endpoint {
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if let Some(rest) = s.strip_prefix("unix://") {
            Ok(Endpoint::Unix(PathBuf::from(rest)))
        } else if let Some(rest) = s.strip_prefix("tcp://") {
            Ok(Endpoint::Tcp(rest.to_string()))
        } else if s.contains('/') {
            Ok(Endpoint::Unix(PathBuf::from(s)))
        } else if s.contains(':') {
            Ok(Endpoint::Tcp(s.to_string()))
        } else {
            bail!("cannot parse admin endpoint {s:?} (want unix:///path, tcp://host:port or /path)")
        }
    }

    /// Best-effort default: the usual unix socket if present, else the
    /// conventional TCP admin port.
    pub fn detect() -> Endpoint {
        for p in ["/var/run/yggdrasil.sock", "/run/yggdrasil.sock"] {
            if std::path::Path::new(p).exists() {
                return Endpoint::Unix(PathBuf::from(p));
            }
        }
        Endpoint::Tcp("127.0.0.1:9001".into())
    }

    fn connect_blocking(&self, timeout: Duration) -> Result<Box<dyn ReadWrite>> {
        match self {
            #[cfg(unix)]
            Endpoint::Unix(path) => {
                let s = UnixStream::connect(path)
                    .with_context(|| format!("connecting to unix socket {}", path.display()))?;
                s.set_read_timeout(Some(timeout))?;
                Ok(Box::new(s))
            }
            #[cfg(not(unix))]
            Endpoint::Unix(path) => {
                let _ = path;
                bail!("unix sockets are not supported on this platform; use tcp://")
            }
            Endpoint::Tcp(addr) => {
                let s = TcpStream::connect(addr)
                    .with_context(|| format!("connecting to tcp {addr}"))?;
                s.set_read_timeout(Some(timeout))?;
                s.set_write_timeout(Some(timeout))?;
                Ok(Box::new(s))
            }
        }
    }
}

trait ReadWrite: Read + Write {}
#[cfg(unix)]
impl ReadWrite for UnixStream {}
impl ReadWrite for TcpStream {}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct GetSelfR {
    #[serde(rename = "build_name", default)]
    pub build_name: String,
    #[serde(rename = "build_version", default)]
    pub build_version: String,
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "address")]
    pub address: String,
    #[serde(rename = "routing_entries", default)]
    pub routing_entries: u64,
    #[serde(rename = "subnet", default)]
    pub subnet: String,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)] // full wire model; not every field is consumed yet
pub struct PeerEntry {
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "port", default)]
    pub port: u64,
    #[serde(rename = "cost", default)]
    pub cost: u64,
    #[serde(rename = "latency", default, deserialize_with = "latency_ns")]
    pub latency_ns: Option<u64>,
    #[serde(rename = "up", default = "default_true")]
    pub up: bool,
    #[serde(rename = "inbound", default)]
    pub inbound: bool,
    #[serde(rename = "remote")]
    pub uri: Option<String>,
}

fn default_true() -> bool {
    true
}

/// yggdrasil-go sends integer nanoseconds (time.Duration), Yggdrasil-ng
/// float milliseconds; serde_json keeps the two number kinds apart.
fn latency_ns<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    let n: Option<serde_json::Number> = serde::Deserialize::deserialize(d)?;
    Ok(n.and_then(|n| n.as_u64().or_else(|| n.as_f64().map(|ms| (ms * 1e6) as u64))))
}

#[derive(Debug, serde::Deserialize)]
pub struct TreeEntry {
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "parent")]
    pub parent: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct PathEntry {
    #[serde(rename = "key")]
    pub key: String,
    #[serde(rename = "path")]
    pub path: Vec<u64>,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct SessionEntry {
    #[serde(rename = "address")]
    pub address: String,
    #[serde(rename = "key")]
    pub key: String,
}

/// Result of a `debug_remoteGetSelf` probe.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RemoteSelf {
    pub key: Key,
    pub routing_entries: Option<u64>,
    pub rtt: Duration,
}

#[derive(Clone)]
pub struct AdminClient {
    endpoint: Endpoint,
    timeout: Duration,
    gate: Arc<Semaphore>,
}

impl AdminClient {
    pub fn new(endpoint: Endpoint, timeout: Duration, parallelism: usize) -> Self {
        AdminClient {
            endpoint,
            timeout,
            gate: Arc::new(Semaphore::new(parallelism.max(1))),
        }
    }

    /// Send one admin request and return the unwrapped `response` field.
    pub async fn request(&self, name: &str, args: Value) -> Result<Value> {
        self.request_with_timeout(name, args, self.timeout).await
    }

    pub async fn request_with_timeout(
        &self,
        name: &str,
        args: Value,
        timeout: Duration,
    ) -> Result<Value> {
        let _permit = self
            .gate
            .acquire()
            .await
            .map_err(|_| anyhow!("admin client shut down"))?;
        let endpoint = self.endpoint.clone();
        let name = name.to_string();
        let name_for_err = name.clone();
        let fut = tokio::task::spawn_blocking(move || -> Result<Value> {
            let mut stream = endpoint.connect_blocking(timeout)?;
            let req = json!({"request": name, "arguments": args});
            let mut buf = serde_json::to_vec(&req)?;
            buf.push(b'\n');
            stream.write_all(&buf)?;
            stream.flush()?;
            let mut reader = std::io::BufReader::new(stream);
            let de = serde_json::Deserializer::from_reader(&mut reader);
            let mut it = de.into_iter::<Value>();
            match it.next() {
                Some(Ok(v)) => Ok(v),
                Some(Err(e)) => Err(anyhow!("invalid admin response: {e}")),
                None => Err(anyhow!("admin socket closed before response")),
            }
        });
        let resp = tokio::time::timeout(timeout + Duration::from_millis(500), fut)
            .await
            .map_err(|_| anyhow!("admin request '{name_for_err}' timed out after {timeout:?}"))???;
        if resp.get("status").and_then(Value::as_str) == Some("success") {
            Ok(resp.get("response").cloned().unwrap_or(Value::Null))
        } else {
            let err = resp
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown admin error");
            Err(anyhow!("admin '{name_for_err}': {err}"))
        }
    }

    pub async fn get_self(&self) -> Result<GetSelfR> {
        let v = self.request("getSelf", json!({})).await?;
        Ok(serde_json::from_value(v)?)
    }

    pub async fn get_peers(&self) -> Result<Vec<PeerEntry>> {
        let v = self.request("getPeers", json!({})).await?;
        #[derive(serde::Deserialize)]
        struct R {
            #[serde(default)]
            peers: Vec<PeerEntry>,
        }
        Ok(serde_json::from_value::<R>(v)?.peers)
    }

    pub async fn get_tree(&self) -> Result<Vec<TreeEntry>> {
        let v = self.request("getTree", json!({})).await?;
        #[derive(serde::Deserialize)]
        struct R {
            #[serde(default)]
            tree: Vec<TreeEntry>,
        }
        Ok(serde_json::from_value::<R>(v)?.tree)
    }

    pub async fn get_paths(&self) -> Result<Vec<PathEntry>> {
        let v = self.request("getPaths", json!({})).await?;
        #[derive(serde::Deserialize)]
        struct R {
            #[serde(default)]
            paths: Vec<PathEntry>,
        }
        Ok(serde_json::from_value::<R>(v)?.paths)
    }

    pub async fn get_sessions(&self) -> Result<Vec<SessionEntry>> {
        let v = self.request("getSessions", json!({})).await?;
        #[derive(serde::Deserialize)]
        struct R {
            #[serde(default)]
            sessions: Vec<SessionEntry>,
        }
        Ok(serde_json::from_value::<R>(v)?.sessions)
    }

    /// Probe a remote node's self info over the overlay; measures round-trip
    /// time from *this* node (not per-hop link latency).
    pub async fn debug_remote_get_self(&self, key: &Key) -> Result<RemoteSelf> {
        let started = std::time::Instant::now();
        let v = self
            .request(
                "debug_remoteGetSelf",
                json!({ "key": key.hex() }),
            )
            .await?;
        let rtt = started.elapsed();
        // Response shape: { "<remote ipv6>": {"key": "...", "routing_entries": "123"} }
        let inner = v
            .as_object()
            .and_then(|m| m.values().next())
            .cloned()
            .unwrap_or(Value::Null);
        let k = inner
            .get("key")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_default();
        let re = inner
            .get("routing_entries")
            .and_then(Value::as_str)
            .and_then(|s| s.parse().ok());
        Ok(RemoteSelf {
            key: Key::from_hex(&k)?,
            routing_entries: re,
            rtt,
        })
    }

    /// Fetch the list of peer *keys* of a remote node (no ports/costs remotely).
    pub async fn debug_remote_get_peers(&self, key: &Key) -> Result<Vec<Key>> {
        let v = self
            .request("debug_remoteGetPeers", json!({ "key": key.hex() }))
            .await?;
        // Response shape: { "<remote ipv6>": {"keys": ["hex", ...]} }
        let inner = v
            .as_object()
            .and_then(|m| m.values().next())
            .cloned()
            .unwrap_or(Value::Null);
        let keys = inner
            .get("keys")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str())
                    .filter_map(|s| Key::from_hex(s).ok())
                    .collect()
            })
            .unwrap_or_default();
        Ok(keys)
    }

    /// Fetch the tree-view of a remote node: the KEYS in its `infos`
    /// (its own ancestry + every peer + peers' ancestries). No parent/port
    /// data remotely — keys only, same envelope as GetPeers.
    pub async fn debug_remote_get_tree(&self, key: &Key) -> Result<Vec<Key>> {
        let v = self
            .request("debug_remoteGetTree", json!({ "key": key.hex() }))
            .await?;
        let inner = v
            .as_object()
            .and_then(|m| m.values().next())
            .cloned()
            .unwrap_or(Value::Null);
        let keys = inner
            .get("keys")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str())
                    .filter_map(|s| Key::from_hex(s).ok())
                    .collect()
            })
            .unwrap_or_default();
        Ok(keys)
    }

    pub async fn get_node_info(&self, key: &Key) -> Result<Value> {
        self.request("getNodeInfo", json!({ "key": key.hex() }))
            .await
    }

    /// Fire-and-forget request: the response is abandoned. Used for bulk
    /// coordinate learning, where the *side effect* on the local node
    /// (path lookup triggered by sending) is what we want. Bypasses the
    /// concurrency semaphore; the server-side handler simply times out.
    pub fn fire(&self, name: &str, args: Value, timeout: Duration) {
        let endpoint = self.endpoint.clone();
        let name = name.to_string();
        std::thread::spawn(move || {
            let _ = (|| -> Result<()> {
                let mut stream = endpoint.connect_blocking(timeout)?;
                let mut buf =
                    serde_json::to_vec(&json!({"request": name, "arguments": args}))?;
                buf.push(b'\n');
                stream.write_all(&buf)?;
                stream.flush()?;
                // Drain until the server responds or times out; we do not care.
                let mut sink = [0u8; 4096];
                let _ = stream.read(&mut sink);
                Ok(())
            })();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_latency_go_and_ng() {
        let p = |j: &str| serde_json::from_str::<PeerEntry>(j).unwrap().latency_ns;
        assert_eq!(p(r#"{"key":"a","latency":96075800}"#), Some(96_075_800)); // go: ns
        assert_eq!(p(r#"{"key":"a","latency":96.0758}"#), Some(96_075_800)); // ng: ms
        assert_eq!(p(r#"{"key":"a"}"#), None);
    }
}

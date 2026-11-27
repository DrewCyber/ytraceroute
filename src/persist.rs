//! Persistent on-disk node cache so that repeated or similar traces start
//! from what was learned last run.
//!
//! `ytraceroute_cache.json` directly in the base directory — the user TEMP
//! on Windows (the user-scope registry value, not the process one),
//! $XDG_CACHE_HOME on Linux/macOS, the current directory as the fallback.
//! Written atomically (tmp + rename), LRU-trimmed by last_seen. Entries
//! keep epoch-ms timestamps; freshness is judged against --cache-ttl at
//! use time, so an old file degrades gracefully into stale hints.

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

/// Cache file name, placed directly in the base directory — no per-app
/// subdirectory.
const CACHE_FILE_NAME: &str = "ytraceroute_cache.json";

/// Default cache location, `<base>/ytraceroute_cache.json`:
/// - Windows: the *user-scope* TEMP directory — the `HKCU\Environment\TEMP`
///   registry value, i.e. what .NET's
///   `[Environment]::GetEnvironmentVariable("TEMP", "User")` returns. The
///   process `TEMP` is deliberately not used: for services or elevated
///   shells it points at the system temp instead of the user's own.
/// - Linux/macOS: `$XDG_CACHE_HOME` (its root, no subdirectory).
/// - Fallback everywhere: the current working directory.
pub fn default_cache_path() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        user_temp_dir().map(|d| d.join(CACHE_FILE_NAME))
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("XDG_CACHE_HOME")
            .filter(|v| !v.is_empty())
            .map(|base| PathBuf::from(base).join(CACHE_FILE_NAME))
            .or_else(cwd_cache_path)
    }
}

/// The spec fallback: `ytraceroute_cache.json` in the current working
/// directory.
#[cfg(not(windows))]
fn cwd_cache_path() -> Option<PathBuf> {
    std::env::current_dir().ok().map(|d| d.join(CACHE_FILE_NAME))
}

/// The user-scope TEMP directory on Windows.
#[cfg(windows)]
fn user_temp_dir() -> Option<PathBuf> {
    use winreg::enums::HKEY_CURRENT_USER;
    use winreg::RegKey;
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let env = hkcu.open_subkey("Environment").ok()?;
    let raw: String = env.get_value("TEMP").ok()?;
    // The value is typically REG_EXPAND_SZ ("%USERPROFILE%\AppData\Local\Temp").
    let dir = PathBuf::from(expand_percent_vars(&raw));
    if dir.is_absolute() {
        Some(dir)
    } else {
        None
    }
}

/// Expand `%VAR%` references using the process environment; unknown names
/// are kept literal.
#[cfg(windows)]
fn expand_percent_vars(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(open) = rest.find('%') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after.find('%') {
            Some(close) => {
                let name = &after[..close];
                match std::env::var(name) {
                    Ok(v) => out.push_str(&v),
                    Err(_) => {
                        out.push('%');
                        out.push_str(name);
                        out.push('%');
                    }
                }
                rest = &after[close + 1..];
            }
            None => {
                // No closing '%': keep it literal.
                out.push('%');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::expand_percent_vars;

    #[test]
    fn percent_expansion() {
        std::env::set_var("YTR_TEST_VAR", "hello");
        assert_eq!(expand_percent_vars("%YTR_TEST_VAR%\\sub"), "hello\\sub");
        assert_eq!(expand_percent_vars("plain"), "plain");
        assert_eq!(expand_percent_vars("%NO_SUCH_VAR%"), "%NO_SUCH_VAR%");
        assert_eq!(expand_percent_vars("a%b"), "a%b");
        assert_eq!(expand_percent_vars("%YTR_TEST_VAR%"), "hello");
    }
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

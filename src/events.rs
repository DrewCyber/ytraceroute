//! Event model shared by all renderers (stream / JSON / TUI).

use std::net::Ipv6Addr;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::mpsc;

use crate::routing::HopKind;
use crate::ygg::{Coords, Key};

/// Serialize a Duration as whole milliseconds (JSON friendliness).
pub mod dur_ms {
    pub fn serialize<S: serde::Serializer>(
        d: &std::time::Duration,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_millis() as u64)
    }
}

/// Serialize an Option<Duration> as whole milliseconds or null.
pub mod dur_opt_ms {
    pub fn serialize<S: serde::Serializer>(
        d: &Option<std::time::Duration>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        match d {
            None => s.serialize_none(),
            Some(d) => s.serialize_u64(d.as_millis() as u64),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum Dir {
    Forward,
    Reverse,
}

impl Dir {
    pub fn label(&self) -> &'static str {
        match self {
            Dir::Forward => "forward",
            Dir::Reverse => "reverse",
        }
    }
}

/// One row of the speculative tree route (via root / LCA).
#[derive(Debug, Clone, Serialize)]
pub struct SpecHop {
    pub key: Option<Key>,
    pub coords: Coords,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum Event {
    /// Bootstrap complete, both endpoints known.
    Started {
        dest_input: String,
        self_key: Key,
        self_address: Ipv6Addr,
        self_coords: Coords,
        dest_key: Key,
        dest_address: Ipv6Addr,
        dest_coords: Coords,
        #[serde(serialize_with = "dur_opt_ms::serialize", skip_serializing_if = "Option::is_none")]
        dest_rtt: Option<Duration>,
        dest_stale: bool,
        cache_loaded: usize,
    },
    /// Initial "via root" guess for a direction.
    SpecRoute {
        dir: Dir,
        route: Vec<SpecHop>,
    },
    /// Progress noise (querying peers / probing coords).
    Querying {
        dir: Dir,
        what: String,
        key: Key,
        #[serde(skip)]
        detail: String,
    },
    /// A hop was confirmed by the routing simulation.
    Hop {
        dir: Dir,
        n: usize,
        key: Key,
        coords: Coords,
        kind: HopKind,
        d_before: u64,
        d_after: u64,
        #[serde(serialize_with = "dur_opt_ms::serialize", skip_serializing_if = "Option::is_none")]
        rtt: Option<Duration>,
        stale: bool,
        cached: bool,
    },
    /// A confirmed hop jumped ahead of the tree route (shortcut).
    ShortcutApplied {
        dir: Dir,
        at_hop: usize,
        skipped: usize,
        from_d: u64,
        to_d: u64,
    },
    /// The target coordinates changed mid-walk: the direction restarts from
    /// its origin; previously shown hops are superseded.
    Restarted {
        dir: Dir,
        msg: String,
    },
    /// Non-fatal issue.
    Warn {
        dir: Option<Dir>,
        msg: String,
    },
    /// The walk could not continue.
    DeadEnd {
        dir: Dir,
        at: Key,
        coords: Option<Coords>,
        msg: String,
    },
    /// Direction finished.
    Done {
        dir: Dir,
        hops: usize,
        delivered: bool,
        #[serde(serialize_with = "dur_ms::serialize")]
        elapsed: Duration,
    },
    /// Optional nodeinfo attached to a hop (--nodeinfo).
    NodeInfoAttached {
        dir: Dir,
        key: Key,
        build_name: Option<String>,
        build_version: Option<String>,
    },
    /// Everything finished; cache was saved.
    Finished {
        #[serde(serialize_with = "dur_ms::serialize")]
        elapsed: Duration,
        saved_nodes: usize,
        cache_path: Option<String>,
    },
}

pub type EventTx = mpsc::Sender<Event>;
pub type EventRx = mpsc::Receiver<Event>;

//! CLI definition.

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Clone, Debug)]
#[command(
    name = "ytraceroute",
    version,
    about = "Predictive traceroute for Yggdrasil (v0.5.x) networks",
    long_about = "ytraceroute predicts the path a packet takes through a Yggdrasil network by \
simulating the ironwood greedy tree-routing decision at every hop, using the admin API \
(debug_remoteGetSelf/GetPeers) to inspect remote nodes. Forward and reverse paths are \
traced in parallel; they may legitimately differ."
)]
pub struct Opts {
    /// Destination: IPv6 address (02.. node or 03.. routed subnet), hex public key, or "self"
    pub dest: String,

    /// Admin socket endpoint: unix:///var/run/yggdrasil.sock, tcp://host:port or a bare path.
    /// [$YTRACEROUTE_ENDPOINT]
    #[arg(short, long)]
    pub endpoint: Option<String>,

    /// Maximum hops per direction
    #[arg(long, default_value_t = 64)]
    pub max_hops: usize,

    /// Concurrent admin requests
    #[arg(long, default_value_t = 4)]
    pub parallelism: usize,

    /// Per-request admin timeout, seconds (server caps debug_remote at 6s)
    #[arg(long, default_value_t = 8.0)]
    pub timeout: f64,

    /// Overall deadline for both walks, seconds
    #[arg(long, default_value_t = 90)]
    pub deadline: u64,

    /// Trace only the forward path
    #[arg(long)]
    pub no_reverse: bool,

    /// Plain streaming output even on a terminal
    #[arg(long)]
    pub no_tui: bool,

    /// NDJSON event stream instead of human output
    #[arg(long)]
    pub json: bool,

    /// Fetch nodeinfo for each confirmed hop
    #[arg(long)]
    pub nodeinfo: bool,

    /// Disable peer-list prefetching
    #[arg(long)]
    pub no_prefetch: bool,

    /// Always probe every peer at every hop (slower; hinted probing may
    /// skip bulk peers after a candidate is found)
    #[arg(long)]
    pub exact: bool,

    /// Node data TTL for the on-disk cache, seconds
    #[arg(long, default_value_t = 90)]
    pub cache_ttl: u64,

    /// Do not read or write the on-disk cache
    #[arg(long)]
    pub no_cache: bool,

    /// Custom cache file location
    #[arg(long)]
    pub cache_file: Option<PathBuf>,

    /// Verbose diagnostics
    #[arg(short, long)]
    pub verbose: bool,
}

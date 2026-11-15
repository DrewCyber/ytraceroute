//! Plain streaming renderer: one line per event as it arrives (works over
//! ssh without a terminal, or with --no-tui).

use std::collections::HashMap;
use std::io::IsTerminal;
use std::io::Write;

use crate::events::{Dir, Event, EventRx};
use crate::render::{fmt_coords, fmt_dur};
use crate::ygg::{addr_for_key, Key};

struct C(bool);

impl C {
    fn dim(&self, s: &str) -> String {
        if self.0 {
            format!("\x1b[2m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
    fn bold(&self, s: &str) -> String {
        if self.0 {
            format!("\x1b[1m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
    fn yellow(&self, s: &str) -> String {
        if self.0 {
            format!("\x1b[33m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
    fn green(&self, s: &str) -> String {
        if self.0 {
            format!("\x1b[32m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
    fn red(&self, s: &str) -> String {
        if self.0 {
            format!("\x1b[31m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
    fn cyan(&self, s: &str) -> String {
        if self.0 {
            format!("\x1b[36m{s}\x1b[0m")
        } else {
            s.to_string()
        }
    }
}

fn dir_arrow(c: &C, dir: Dir) -> String {
    match dir {
        Dir::Forward => c.cyan("→"),
        Dir::Reverse => c.yellow("←"),
    }
}

/// Consume events until the stream closes; returns a process exit code.
pub async fn run(mut rx: EventRx, verbose: bool) -> i32 {
    let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    // conhost starts with VT processing off: enable it, or print plain text.
    #[cfg(windows)]
    let color = color && crossterm::ansi_support::supports_ansi();
    let c = C(color);
    let mut out = std::io::stdout().lock();
    let mut done: HashMap<Dir, bool> = HashMap::new();
    let mut fwd_hops = 0usize;
    let mut rev_hops = 0usize;

    while let Some(ev) = rx.recv().await {
        let line = match &ev {
            Event::Started {
                dest_input,
                self_key,
                self_address,
                self_coords,
                dest_key,
                dest_address,
                dest_coords,
                dest_rtt,
                dest_stale,
                cache_loaded,
            } => {
                let mut s = String::new();
                s.push_str(&c.bold(&format!(
                    "ytraceroute to {dest_input}"
                )));
                s.push_str(&format!(
                    "  (key {}…)\n",
                    &dest_key.hex()[..8]
                ));
                s.push_str(&format!(
                    "  you:  {} {}\n",
                    self_address,
                    c.dim(&fmt_coords(self_coords))
                ));
                let stale_mark = if *dest_stale { c.yellow(" [stale]") } else { String::new() };
                let rtt = dest_rtt
                    .as_ref()
                    .map(|r| format!("  rtt {}", fmt_dur(*r)))
                    .unwrap_or_default();
                s.push_str(&format!(
                    "  dest: {} {}{}{}\n",
                    dest_address,
                    c.dim(&fmt_coords(dest_coords)),
                    stale_mark,
                    rtt
                ));
                if *cache_loaded > 0 {
                    s.push_str(&c.dim(&format!(
                        "  cache: {cache_loaded} nodes loaded\n"
                    )));
                }
                let _ = self_key;
                s
            }
            Event::SpecRoute { dir, route } => c.dim(&format!(
                "{} speculative {} route via tree: {} hops (refines as hops confirm)",
                dir_arrow(&c, *dir),
                dir.label(),
                route.len()
            )),
            Event::Querying { dir, what, key, .. } if verbose => c.dim(&format!(
                "{} {} {}: {}",
                dir_arrow(&c, *dir),
                dir.label(),
                what,
                short(key)
            )),
            Event::Querying { .. } => continue,
            Event::Hop {
                dir,
                n,
                key,
                coords,
                kind,
                d_before,
                d_after,
                rtt,
                stale,
                cached,
                trust,
                trust_notes,
            } => {
                if *dir == Dir::Forward {
                    fwd_hops = fwd_hops.max(*n);
                } else {
                    rev_hops = rev_hops.max(*n);
                }
                let cached_mark = if *cached { c.dim(" ⛁") } else { String::new() };
                let stale_mark = if *stale { c.yellow(" ~") } else { String::new() };
                let rtt_s = rtt
                    .as_ref()
                    .map(|r| fmt_dur(*r))
                    .unwrap_or_else(|| "-".to_string());
                let (jump_pre, rtt_rest) = crate::render::jump_rtt_cell(
                    matches!(kind, crate::routing::HopKind::Shortcut),
                    &rtt_s,
                );
                let rtt = format!("{}{}", c.yellow(&jump_pre), rtt_rest);
                let trust_mark = match trust {
                    crate::routing::TrustLevel::High => String::new(),
                    crate::routing::TrustLevel::Medium => c.yellow(" ?").to_string(),
                    crate::routing::TrustLevel::Low => c.red(" \u{2020}").to_string(),
                };
                let trust_note = if verbose && !trust_notes.is_empty() {
                    c.dim(&format!("  \u{b7} {}", trust_notes.join("; ")))
                } else {
                    String::new()
                };
                format!(
                    "{} {:>2}  {}  {}  {:<22} {d_before}->{d_after} {rtt}{cached_mark}{stale_mark}{trust_mark}{trust_note}",
                    dir_arrow(&c, *dir),
                    n,
                    addr_for_key(key),
                    c.dim(&short(key)),
                    c.dim(&fmt_coords(coords)),
                )
            }
            Event::ShortcutApplied {
                dir,
                at_hop,
                skipped,
                from_d,
                to_d,
            } => format!(
                "{} {}",
                dir_arrow(&c, *dir),
                c.yellow(&format!(
                    "⚡ hop {at_hop}: jump, skipped {skipped} tree hops ({from_d}→{to_d})"
                ))
            ),
            Event::Restarted { dir, msg } => format!(
                "{} {}",
                dir_arrow(&c, *dir),
                c.cyan(&format!("↻ {msg}"))
            ),
            Event::Warn { msg, .. } => format!("{} {}", c.yellow("!"), c.dim(msg)),
            Event::DeadEnd { dir, at, msg, .. } => format!(
                "{} {}",
                dir_arrow(&c, *dir),
                c.red(&format!("✗ dead end at {}: {msg}", short(at)))
            ),
            Event::Verdict {
                dir,
                level,
                high,
                medium,
                low,
                delivered,
                notes,
            } => {
                let (word, col) = match level {
                    crate::routing::TrustLevel::High => ("high", c.green("high")),
                    crate::routing::TrustLevel::Medium => ("medium", c.yellow("medium")),
                    crate::routing::TrustLevel::Low => ("low", c.red("low")),
                };
                let _ = word;
                let total = high + medium + low;
                let mut line = format!(
                    "{} {} route confidence: {} \u{b7} {total} hops (high {high}, medium {medium}, low {low})",
                    dir_arrow(&c, *dir),
                    dir.label(),
                    col,
                );
                if !*delivered {
                    line.push_str(&c.dim(" \u{b7} incomplete"));
                }
                if verbose && !notes.is_empty() {
                    line.push_str(&c.dim(&format!(" \u{b7} {}", notes.join("; "))));
                }
                line
            }
            Event::Done {
                dir,
                hops,
                delivered,
                elapsed,
            } => {
                done.insert(*dir, *delivered);
                let mark = if *delivered {
                    c.green("✔")
                } else {
                    c.red("✗")
                };
                format!(
                    "{} {} {}: {hops} hops, {} in {}",
                    mark,
                    dir_arrow(&c, *dir),
                    dir.label(),
                    if *delivered { "delivered" } else { "incomplete" },
                    fmt_dur(*elapsed)
                )
            }
            Event::NodeInfoAttached {
                key,
                build_name,
                build_version,
                ..
            } => c.dim(&format!(
                "  ℹ {} = {} {}",
                short(key),
                build_name.as_deref().unwrap_or("?"),
                build_version.as_deref().unwrap_or("?")
            )),
            Event::Finished {
                elapsed,
                saved_nodes,
                cache_path,
            } => {
                let mut s = format!(
                    "{} traced in {}",
                    c.bold("done"),
                    fmt_dur(*elapsed)
                );
                if fwd_hops > 0 || rev_hops > 0 {
                    s.push_str(&format!("  (fwd {fwd_hops}"));
                    if rev_hops > 0 {
                        s.push_str(&format!(", rev {rev_hops}"));
                    }
                    s.push(')');
                }
                if let (Some(p), true) = (cache_path, *saved_nodes > 0) {
                    s.push_str(&c.dim(&format!("\n  cache: {saved_nodes} nodes → {p}")));
                }
                s
            }
        };
        let _ = writeln!(out, "{line}");
        let _ = out.flush();
    }
    if done.is_empty() {
        2
    } else if done.values().all(|v| *v) {
        0
    } else {
        1
    }
}

fn short(k: &Key) -> String {
    format!("{}…", k.short())
}

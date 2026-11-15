//! Interactive dual-pane TUI (ratatui + crossterm).
//!
//! Forward and reverse walks update their panes live as events arrive:
//! confirmed hops replace the dim speculative tree-route rows, shortcuts
//! flash yellow, and the status bar shows what is being queried right now.

use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::event::{Event as CEvent, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::execute;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Terminal;
use tokio::sync::mpsc;

use crate::events::{Dir, Event, EventRx, SpecHop};
use crate::routing::HopKind;
use crate::render::{fmt_coords, fmt_dur};
use crate::ygg::{addr_for_key, tree_dist, Coords, Key};

#[derive(Clone)]
struct HopRow {
    n: usize,
    key: Key,
    coords: Coords,
    kind: HopKind,
    d_before: u64,
    d_after: u64,
    rtt: Option<Duration>,
    nodeinfo: Option<(String, String)>,
    trust: crate::routing::TrustLevel,
    trust_notes: Vec<String>,
}

#[derive(Default)]
struct DirState {
    /// Row 0: the node the simulated packet starts from.
    start: Option<(String, Coords, String)>,
    spec: Vec<SpecHop>,
    hops: Vec<HopRow>,
    notes: Vec<String>,
    deadend: Option<String>,
    done: Option<(usize, bool, Duration)>,
    verdict: Option<(String, Vec<String>)>,
    status: Option<String>,
    target_coords: Option<Coords>,
}

impl DirState {
    fn apply_hop(&mut self, ev: &Event) {
        if let Event::Hop {
            n,
            key,
            coords,
            kind,
            d_before,
            d_after,
            rtt,
            trust,
            trust_notes,
            ..
        } = ev
        {
            self.hops.push(HopRow {
                n: *n,
                key: *key,
                coords: coords.clone(),
                kind: *kind,
                d_before: *d_before,
                d_after: *d_after,
                rtt: *rtt,
                nodeinfo: None,
                trust: *trust,
                trust_notes: trust_notes.clone(),
            });
            // Everything the shortcut jumped over is no longer plausible.
            if let Some(t) = &self.target_coords {
                self.spec
                    .retain(|s| tree_dist(&s.coords, t) < *d_after);
            }
        }
    }
}

struct TuiState {
    started: Instant,
    banner: Option<Box<crate::events::Event>>,
    fwd: DirState,
    rev: DirState,
    warnings: Vec<String>,
    finished: Option<(Duration, usize, Option<String>)>,
    quit: bool,
    focus: Dir,
    single_pane: bool,
    show_details: bool,
    scroll_fwd: u16,
    scroll_rev: u16,
}

impl TuiState {
    fn new() -> Self {
        TuiState {
            started: Instant::now(),
            banner: None,
            fwd: DirState::default(),
            rev: DirState::default(),
            warnings: Vec::new(),
            finished: None,
            quit: false,
            focus: Dir::Forward,
            single_pane: false,
            show_details: false,
            scroll_fwd: 0,
            scroll_rev: 0,
        }
    }

    fn dir_mut(&mut self, d: Dir) -> &mut DirState {
        match d {
            Dir::Forward => &mut self.fwd,
            Dir::Reverse => &mut self.rev,
        }
    }

    fn apply(&mut self, ev: Event) {
        match &ev {
            Event::Started {
                dest_coords,
                self_coords,
                dest_address,
                self_address,
                ..
            } => {
                self.banner = Some(Box::new(ev.clone()));
                self.fwd.start = Some((
                    self_address.to_string(),
                    self_coords.clone(),
                    "you".into(),
                ));
                self.rev.start = Some((
                    dest_address.to_string(),
                    dest_coords.clone(),
                    "dest".into(),
                ));
                self.fwd.target_coords = Some(dest_coords.clone());
                self.rev.target_coords = Some(self_coords.clone());
            }
            Event::SpecRoute { dir, route } => {
                self.dir_mut(*dir).spec = route.clone();
            }
            Event::Hop { dir, .. } => self.dir_mut(*dir).apply_hop(&ev),
            Event::ShortcutApplied {
                dir,
                at_hop,
                skipped,
                from_d,
                to_d,
            } => {
                self.dir_mut(*dir).notes.push(format!(
                    "⚡ hop {at_hop}: jump, skipped {skipped} tree hops ({from_d}→{to_d})"
                ));
            }
            Event::Querying { dir, what, key, .. } => {
                self.dir_mut(*dir).status = Some(format!("{what}: {}", key.short()));
            }
            Event::Restarted { dir, msg } => {
                // The old rows were computed toward stale target coords:
                // drop them, the restarted walk re-emits fresh hops.
                let st = self.dir_mut(*dir);
                st.hops.clear();
                st.spec.clear();
                st.deadend = None;
                st.done = None;
                st.notes.clear();
                st.notes.push(format!("↻ {msg}"));
            }
            Event::Warn { msg, .. } => {
                if self.warnings.len() < 5 {
                    self.warnings.push(msg.clone());
                }
            }
            Event::DeadEnd { dir, at, msg, .. } => {
                self.dir_mut(*dir).deadend = Some(format!("✗ dead end at {}: {msg}", at.short()));
            }
            Event::Verdict {
                dir,
                level,
                high,
                medium,
                low,
                notes,
                ..
            } => {
                let total = high + medium + low;
                let header = format!(
                    "\u{2248} route confidence: {level:?} \u{b7} {total} hops (high {high}, medium {medium}, low {low})"
                );
                self.dir_mut(*dir).verdict = Some((header, notes.clone()));
            }
            Event::Done {
                dir,
                hops,
                delivered,
                elapsed,
            } => {
                self.dir_mut(*dir).done = Some((*hops, *delivered, *elapsed));
                self.dir_mut(*dir).status = None;
            }
            Event::NodeInfoAttached { dir, key, .. } => {
                if let Event::NodeInfoAttached {
                    build_name,
                    build_version,
                    ..
                } = &ev
                {
                    let st = self.dir_mut(*dir);
                    if let Some(h) = st.hops.iter_mut().find(|h| h.key == *key) {
                        h.nodeinfo = Some((
                            build_name.clone().unwrap_or_default(),
                            build_version.clone().unwrap_or_default(),
                        ));
                    }
                }
            }
            Event::Finished {
                elapsed,
                saved_nodes,
                cache_path,
            } => {
                self.finished = Some((*elapsed, *saved_nodes, cache_path.clone()));
            }
        }
    }

    fn exit_code(&self) -> i32 {
        let f = self.fwd.done.as_ref().map(|(_, d, _)| *d);
        let r = self.rev.done.as_ref().map(|(_, d, _)| *d);
        match (f, r) {
            (Some(true), None) | (None, Some(true)) | (Some(true), Some(true)) => 0,
            (None, None) => 2,
            _ => 1,
        }
    }
}

/// Per-depth column widths so all coordinate paths in a pane line up:
/// shared prefixes form a visible vertical spine.
fn coord_widths(rows: &[&Coords]) -> Vec<usize> {
    let mut w: Vec<usize> = Vec::new();
    for c in rows {
        for (i, p) in c.iter().enumerate() {
            let l = p.to_string().len();
            if i == w.len() {
                w.push(l);
            } else if l > w[i] {
                w[i] = l;
            }
        }
    }
    w
}

fn fmt_coords_cols(c: &[u64], w: &[usize]) -> String {
    let inner = c
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let width = w.get(i).copied().unwrap_or(0);
            format!("{p:<width$}")
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{inner}]")
}

fn hop_lines(st: &DirState, detail: bool) -> Vec<Line<'static>> {
    // Column widths across every row type so the tree depth lines up.
    let mut all: Vec<&Coords> = Vec::new();
    if let Some((_, c, _)) = &st.start {
        all.push(c);
    }
    for h in &st.hops {
        all.push(&h.coords);
    }
    for srow in &st.spec {
        all.push(&srow.coords);
    }
    let widths = coord_widths(&all);
    // The coords STRING length varies (short paths end early); pad the
    // whole field so the distance column after it lines up vertically.
    let coords_w = all
        .iter()
        .map(|c| fmt_coords_cols(c, &widths).len())
        .max()
        .unwrap_or(2)
        .max(2);

    // Fixed address column so the coords start at the same offset on every
    // row regardless of how long each IPv6 address happens to be.
    let addr_w = st
        .hops
        .iter()
        .map(|h| addr_for_key(&h.key).to_string().len())
        .chain(st.start.as_ref().map(|(a, _, _)| a.len()))
        .max()
        .unwrap_or(0)
        .max(20);

    let mut lines = Vec::new();
    // Row 0: the starting node.
    if let Some((addr, c, label)) = &st.start {
        lines.push(Line::from(vec![
            Span::styled(
                format!("{:>2} ", 0),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("{addr:<addr_w$}")),
            Span::styled(
                format!("{:<coords_w$}", fmt_coords_cols(c, &widths)),
                Style::default().fg(Color::DarkGray),
            ),
            Span::styled(format!(" {label}"), Style::default().fg(Color::DarkGray)),
        ]));
    }
    for h in &st.hops {
        // Distance change sits in the same column as the you/dest label;
        // "jump" for shortcut hops is overlaid into the rtt cell's leading
        // blanks so rows without it stay aligned.
        let dist = format!("{}->{}", h.d_before, h.d_after);
        let rtt_s = h
            .rtt
            .as_ref()
            .map(|r| fmt_dur(*r))
            .unwrap_or_else(|| "-".into());
        let (jump_pre, rtt_rest) =
            crate::render::jump_rtt_cell(h.kind == HopKind::Shortcut, &rtt_s);
        let mut spans = vec![
            Span::styled(
                format!("{:>2} ", h.n),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("{:<addr_w$}", addr_for_key(&h.key))),
            Span::styled(
                format!("{:<coords_w$}", fmt_coords_cols(&h.coords, &widths)),
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw(format!(" {dist}")),
        ];
        if !jump_pre.is_empty() {
            spans.push(Span::styled(
                format!(" {jump_pre}"),
                Style::default().fg(Color::Yellow),
            ));
            spans.push(Span::raw(rtt_rest));
        } else {
            // Keep the historical 8-column cell: space + 7-wide rtt.
            spans.push(Span::raw(format!(" {rtt_rest}")));
        }
        match h.trust {
            crate::routing::TrustLevel::High => {}
            crate::routing::TrustLevel::Medium => spans.push(Span::styled(
                " ?",
                Style::default().fg(Color::Yellow),
            )),
            crate::routing::TrustLevel::Low => spans.push(Span::styled(
                " \u{2020}",
                Style::default().fg(Color::Red),
            )),
        }
        if detail && !h.trust_notes.is_empty() {
            spans.push(Span::styled(
                format!(" \u{b7} {}", h.trust_notes.join("; ")),
                Style::default().fg(Color::DarkGray),
            ));
        }
        if let Some((bn, bv)) = &h.nodeinfo {
            spans.push(Span::styled(format!("  ℹ {bn} {bv}"), Style::default().fg(Color::DarkGray)));
        }
        lines.push(Line::from(spans));
    }
    // Shortcut / dead-end / done notes
    for n in &st.notes {
        lines.push(Line::from(Span::styled(
            n.clone(),
            Style::default().fg(Color::Yellow),
        )));
    }
    // Remaining speculative rows
    for srow in &st.spec {
        lines.push(Line::from(vec![
            Span::styled(format!("{:>2} ", "~"), Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:addr_w$}", "")),
            Span::styled(
                format!("{:<coords_w$}", fmt_coords_cols(&srow.coords, &widths)),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
    }
    if let Some(d) = &st.deadend {
        lines.push(Line::from(Span::styled(d.clone(), Style::default().fg(Color::Red))));
    }
    if let Some((header, vnotes)) = &st.verdict {
        let mut line = header.clone();
        if detail && !vnotes.is_empty() {
            line.push_str(&format!(" \u{b7} {}", vnotes.join("; ")));
        }
        lines.push(Line::from(Span::styled(
            line,
            Style::default().fg(Color::DarkGray),
        )));
    }
    if let Some((hops, delivered, elapsed)) = &st.done {
        let (mark, c) = if *delivered {
            ("✔", Color::Green)
        } else {
            ("✗", Color::Red)
        };
        lines.push(Line::from(Span::styled(
            format!("{mark} {} hops, {} in {}", hops, if *delivered { "delivered" } else { "incomplete" }, fmt_dur(*elapsed)),
            Style::default().fg(c).add_modifier(Modifier::BOLD),
        )));
    }
    lines
}

fn dir_block_title(dir: Dir, st: &DirState, dest_label: &str, focused: bool) -> String {
    let (arrow, label) = match dir {
        Dir::Forward => ("→", format!("FORWARD  you → {dest_label}")),
        Dir::Reverse => ("←", format!("REVERSE  {dest_label} → you")),
    };
    let focus_mark = if focused { " ▣" } else { "" };
    let done = st
        .done
        .as_ref()
        .map(|(h, d, _)| format!(" · {h} hops{}", if *d { "" } else { " (incomplete)" }))
        .unwrap_or_default();
    format!("{arrow} {label}{focus_mark}{done}")
}

pub async fn run(mut rx: EventRx, abort: tokio::task::AbortHandle) -> i32 {
    let mut terminal = setup_terminal().expect("terminal setup");
    let mut state = TuiState::new();
    let (key_tx, mut key_rx) = mpsc::channel::<KeyEvent>(16);
    std::thread::spawn(move || -> Result<()> {
        loop {
            if crossterm::event::poll(Duration::from_millis(100))?
                && let CEvent::Key(k) = crossterm::event::read()?
                    // Windows also reports key releases; toggles must fire once.
                    && k.kind == KeyEventKind::Press
                    && key_tx.blocking_send(k).is_err() {
                        break;
                    }
        }
        Ok(())
    });

    let mut closed = false;
    let exit_code = loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                None => { closed = true; }
                Some(e) => state.apply(e),
            },
            k = key_rx.recv() => {
                if let Some(k) = k {
                    match (k.code, k.modifiers) {
                        (KeyCode::Char('q'), _) | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                            state.quit = true;
                        }
                        (KeyCode::Char('c'), _) => {}
                        (KeyCode::Tab, _) => {
                            state.focus = match state.focus {
                                Dir::Forward => Dir::Reverse,
                                Dir::Reverse => Dir::Forward,
                            };
                        }
                        (KeyCode::Char('v'), _) => {
                            state.single_pane = !state.single_pane;
                        }
                        (KeyCode::Char('d'), _) => {
                            state.show_details = !state.show_details;
                        }
                        (KeyCode::Down, _) => {
                            if state.focus == Dir::Forward { state.scroll_fwd = state.scroll_fwd.saturating_add(1); }
                            else { state.scroll_rev = state.scroll_rev.saturating_add(1); }
                        }
                        (KeyCode::Up, _) => {
                            if state.focus == Dir::Forward { state.scroll_fwd = state.scroll_fwd.saturating_sub(1); }
                            else { state.scroll_rev = state.scroll_rev.saturating_sub(1); }
                        }
                        _ => {}
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
        let _ = terminal.draw(|f| draw(f, &state));
        if state.quit {
            break state.exit_code();
        }
        if closed {
            // App finished; wait for the user to close (q), but auto-exit
            // after a while so scripted TUI runs do not hang forever.
            if state.started.elapsed() > Duration::from_secs(300) {
                break state.exit_code();
            }
            // Keep drawing until q.
        }
    };
    restore_terminal(&mut terminal);
    // Stop walks still running after q/Ctrl-C; no-op once they finished.
    abort.abort();
    exit_code
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<std::io::Stdout>>> {
    enable_raw_mode()?;
    execute!(std::io::stdout(), EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(std::io::stdout());
    Ok(Terminal::new(backend)?)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>) {
    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = disable_raw_mode();
}

fn draw(f: &mut ratatui::Frame, state: &TuiState) {
    let (dest_input, self_addr, self_coords, dest_addr, dest_coords, dest_rtt, cache_loaded) =
        match state.banner.as_deref() {
            Some(Event::Started {
                dest_input,
                self_address,
                self_coords,
                dest_address,
                dest_coords,
                dest_rtt,
                cache_loaded,
                ..
            }) => (
                dest_input.clone(),
                self_address.to_string(),
                self_coords.clone(),
                dest_address.to_string(),
                dest_coords.clone(),
                dest_rtt.map(fmt_dur),
                *cache_loaded,
            ),
            _ => (
                "…".into(),
                "…".into(),
                Vec::new(),
                "…".into(),
                Vec::new(),
                None,
                0,
            ),
        };

    // Focus enlarges its pane; single-pane mode ('v') hides the other one.
    let (fwd_share, rev_share) = match (state.single_pane, state.focus) {
        (true, Dir::Forward) => (100, 0),
        (true, Dir::Reverse) => (0, 100),
        (false, Dir::Forward) => (65, 35),
        (false, Dir::Reverse) => (35, 65),
    };
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Percentage(fwd_share),
        Constraint::Percentage(rev_share),
        Constraint::Length(2),
    ])
    .split(f.area());

    // Header
    let header = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("ytraceroute ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::styled(dest_input.clone(), Style::default().add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(self_addr, Style::default().fg(Color::DarkGray)),
            Span::styled(format!(" {}", fmt_coords(&self_coords)), Style::default().fg(Color::DarkGray)),
            Span::raw("  →  "),
            Span::styled(dest_addr.clone(), Style::default().fg(Color::Magenta)),
            Span::styled(format!(" {}", fmt_coords(&dest_coords)), Style::default().fg(Color::DarkGray)),
            dest_rtt.map(|r| Span::raw(format!("  rtt {r}"))).unwrap_or_default(),
        ]),
        Line::from(Span::styled(
            format!(
                "fwd/rev traced in parallel · prediction replicates ironwood greedy tree-routing{}",
                if cache_loaded > 0 { format!(" · cache: {cache_loaded} nodes") } else { String::new() }
            ),
            Style::default().fg(Color::DarkGray),
        )),
    ])
    .wrap(Wrap { trim: true });
    f.render_widget(header, chunks[0]);

    // Direction panes. Skip the hidden one in single-pane mode.
    for ((idx, scroll), (dir, st)) in [
        ((1usize, state.scroll_fwd), (Dir::Forward, &state.fwd)),
        ((2usize, state.scroll_rev), (Dir::Reverse, &state.rev)),
    ] {
        if state.single_pane && state.focus != dir {
            continue;
        }
        let block = match dir {
            Dir::Forward => Block::default().borders(Borders::ALL).title(
                Span::styled(dir_block_title(dir, st, &dest_input, state.focus == dir), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
            ),
            Dir::Reverse => Block::default().borders(Borders::ALL).title(
                Span::styled(dir_block_title(dir, st, &dest_input, state.focus == dir), Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD))
            ),
        };
        let mut lines = hop_lines(st, state.show_details);
        if let Some(s) = &st.status {
            lines.push(Line::from(Span::styled(
                format!("… {s}"),
                Style::default().fg(Color::Yellow),
            )));
        }
        let rect = chunks[idx];
        let inner_height = rect.height.saturating_sub(2) as usize;
        let total = lines.len();
        // Auto-follow: show the tail unless the user scrolled up.
        let skip = if scroll > 0 {
            (total.saturating_sub(inner_height)).saturating_sub(scroll as usize)
        } else {
            total.saturating_sub(inner_height)
        };
        let p = Paragraph::new(lines.into_iter().skip(skip).collect::<Vec<_>>())
            .block(block)
            .wrap(Wrap { trim: false });
        f.render_widget(p, rect);
    }

    // Footer: warnings + controls
    let elapsed = state
        .finished
        .as_ref()
        .map(|(e, _, _)| fmt_dur(*e))
        .unwrap_or_else(|| fmt_dur(state.started.elapsed()));
    let mut footer_lines = vec![Line::from(vec![
        Span::styled(
            format!(" {elapsed}"),
            Style::default().fg(Color::Yellow),
        ),
        Span::styled(
            match state.focus {
                Dir::Forward => "  [fwd]",
                Dir::Reverse => "  [rev]",
            },
            Style::default().fg(Color::DarkGray),
        ),
        Span::styled(
            "  tab:focus  v:single-pane  d:details  ↑↓:scroll  q:quit",
            Style::default().fg(Color::DarkGray),
        ),
        if let Some((_, saved, Some(path))) = &state.finished {
            Span::styled(format!("  ⛁ {saved} → {path}"), Style::default().fg(Color::DarkGray))
        } else {
            Span::raw("")
        },
    ])];
    for w in state.warnings.iter().rev().take(2) {
        footer_lines.push(Line::from(Span::styled(
            format!(" ! {w}"),
            Style::default().fg(Color::DarkGray),
        )));
    }
    let footer = Paragraph::new(footer_lines).wrap(Wrap { trim: true });
    f.render_widget(footer, chunks[3]);

}

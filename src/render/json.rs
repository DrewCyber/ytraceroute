//! NDJSON renderer: one JSON object per line, suitable for scripting.

use std::collections::HashMap;
use std::io::Write;

use crate::events::{Dir, Event, EventRx};

pub async fn run(mut rx: EventRx) -> i32 {
    let mut out = std::io::stdout().lock();
    let mut done: HashMap<Dir, bool> = HashMap::new();
    while let Some(ev) = rx.recv().await {
        let _ = writeln!(out, "{}", serde_json::to_string(&ev).unwrap_or_default());
        let _ = out.flush();
        if let Event::Done { dir, delivered, .. } = &ev {
            done.insert(*dir, *delivered);
        }
    }
    if done.is_empty() {
        2
    } else if done.values().all(|v| *v) {
        0
    } else {
        1
    }
}

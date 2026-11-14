pub mod json;
pub mod stream;
pub mod tui;

use crate::ygg::Coords;

pub fn fmt_coords(c: &Coords) -> String {
    if c.is_empty() {
        "[] (root)".to_string()
    } else {
        format!("[{}]", c.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(","))
    }
}

pub fn fmt_dur(d: std::time::Duration) -> String {
    let ms = d.as_millis();
    if ms < 1000 {
        format!("{ms}ms")
    } else {
        format!("{:.1}s", d.as_secs_f64())
    }
}

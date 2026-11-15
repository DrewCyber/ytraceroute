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

/// Build the distance/rtt cell: rtt right-aligned in 7 columns, with
/// "jump " overlaid onto its leading blank space for shortcut hops. When
/// the rtt text is too wide to leave 5 blanks, the cell simply grows.
pub fn jump_rtt_cell(is_jump: bool, rtt_s: &str) -> (String, String) {
    let cell = format!("{rtt_s:>7}");
    if !is_jump {
        return (String::new(), cell);
    }
    let lead = cell.len() - cell.trim_start().len();
    let take = lead.min(5);
    ("jump ".to_string(), cell[take..].to_string())
}

pub fn fmt_dur(d: std::time::Duration) -> String {
    let ms = d.as_millis();
    if ms < 1000 {
        format!("{ms}ms")
    } else {
        format!("{:.1}s", d.as_secs_f64())
    }
}

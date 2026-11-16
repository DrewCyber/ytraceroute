//! ytraceroute: predictive traceroute for Yggdrasil (v0.5.x) networks.

mod admin;
mod app;
mod events;
mod model;
mod opts;
mod persist;
mod render;
mod routing;
mod walk;
mod ygg;

use std::io::IsTerminal;

use anyhow::Result;
use clap::Parser;

use opts::Opts;

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let use_tui = !opts.no_tui
        && !opts.json
        && std::io::stdout().is_terminal()
        && std::io::stderr().is_terminal();

    let app = app::App::bootstrap(&opts).await?;
    let (handle, rx, cancel) = app.spawn_run();

    let code = if opts.json {
        render::json::run(rx).await
    } else if use_tui {
        render::tui::run(opts.clone(), rx, handle.abort_handle(), cancel).await
    } else {
        render::stream::run(rx, opts.verbose).await
    };

    let res = handle.await;
    if let Ok(Err(e)) = res {
        eprintln!("error: {e:#}");
        std::process::exit(2);
    }
    std::process::exit(code);
}

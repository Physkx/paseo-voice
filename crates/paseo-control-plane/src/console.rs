//! Terminal interface over the Rust tool engine.

use std::{
    io::{BufRead as _, Write as _},
    sync::{Arc, Mutex},
};

use crate::{
    clock::{Clock, SystemClock},
    commands::CommandState,
    config::Config,
    journal::Journal,
    paseo::SystemProcessExecutor,
    secrets::Secrets,
    tools::ToolEngine,
};

/// Run the interactive terminal until EOF or exit.
///
/// # Errors
///
/// Returns an error when journal or terminal I/O fails.
pub fn run(config: &Config, secrets: &Secrets) -> Result<(), String> {
    run_with_clock(config, secrets, &SystemClock::start())
}

/// Run the terminal with an injected monotonic clock.
///
/// # Errors
///
/// Returns an error when journal or terminal I/O fails.
pub fn run_with_clock(config: &Config, secrets: &Secrets, clock: &dyn Clock) -> Result<(), String> {
    if let Some(parent) = config.journal_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                .map_err(|error| error.to_string())?;
        }
    }
    let journal = Journal::open(&config.journal_path).map_err(|error| error.to_string())?;
    journal.recover(0).map_err(|error| error.to_string())?;
    let mut tools = ToolEngine::configured(
        &config.paseo_hosts,
        &SystemProcessExecutor,
        &config.paseo_bin,
        secrets.paseo_password.as_deref(),
        config.proposal_ttl_ms,
    )
    .map_err(str::to_owned)?
    .with_journal(Arc::new(Mutex::new(journal)))
    .with_log_tail_entries(config.log_tail_entries);
    let mut commands = CommandState::default();
    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();
    let mut stdout = std::io::stdout().lock();
    writeln!(
        stdout,
        "paseo-voice Rust console. help for commands, exit to quit."
    )
    .map_err(|error| error.to_string())?;
    loop {
        write!(stdout, "> ").map_err(|error| error.to_string())?;
        stdout.flush().map_err(|error| error.to_string())?;
        let mut line = String::new();
        if stdin
            .read_line(&mut line)
            .map_err(|error| error.to_string())?
            == 0
        {
            break;
        }
        if matches!(line.trim().to_lowercase().as_str(), "exit" | "quit") {
            break;
        }
        tools.observe_user_turn();
        let now_ms = clock.now_ms();
        let result = commands.run(&mut tools, &line, now_ms);
        writeln!(stdout, "{}", result.speech).map_err(|error| error.to_string())?;
    }
    Ok(())
}

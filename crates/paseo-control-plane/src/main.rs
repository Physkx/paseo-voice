#![forbid(unsafe_code)]

use std::env;
use std::process::ExitCode;

use std::collections::HashMap;

use paseo_control_plane::{
    config, paseo::SystemProcessExecutor, protocol::serve_stdio, runtime, secrets::load_secrets,
    version_line,
};

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    match (args.next(), args.next()) {
        (Some(argument), None) if argument == "--version" => {
            println!("{}", version_line());
            ExitCode::SUCCESS
        }
        (Some(argument), None) if argument == "--serve-stdio" => {
            match serve_stdio(std::io::stdin().lock(), std::io::stdout().lock(), 120_000) {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("control-plane stopped: {error}");
                    ExitCode::from(1)
                }
            }
        }
        (Some(argument), None) if argument == "serve" => {
            let environment = env::vars().collect::<HashMap<_, _>>();
            let config = match config::load(&environment) {
                Ok(config) => config,
                Err(error) => {
                    eprintln!("configuration failed: {error}");
                    return ExitCode::from(1);
                }
            };
            let secret_config = match config.secret_config() {
                Ok(secret_config) => secret_config,
                Err(error) => {
                    eprintln!("configuration failed: {error}");
                    return ExitCode::from(1);
                }
            };
            let secrets = load_secrets(&secret_config, &SystemProcessExecutor, &environment);
            match runtime::run(config, secrets).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("runtime stopped: {error}");
                    ExitCode::from(1)
                }
            }
        }
        _ => {
            eprintln!("usage: paseo-control-plane --version | --serve-stdio | serve");
            ExitCode::from(2)
        }
    }
}

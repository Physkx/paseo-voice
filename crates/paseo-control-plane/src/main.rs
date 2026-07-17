#![forbid(unsafe_code)]

use std::env;
use std::process::ExitCode;

use std::collections::HashMap;

use paseo_control_plane::{
    clock::SystemClock, config, console, paseo::SystemProcessExecutor, protocol::serve_stdio,
    realtime::SystemRealtimeConnector, runtime, secrets::load_secrets, version_line,
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
        (Some(argument), None) if argument == "serve" || argument == "console" => {
            let serve = argument == "serve";
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
            if matches!(config.log_level.as_str(), "debug" | "info") {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "level": "info",
                        "event": "runtime_starting",
                        "mode": if secrets.openai_api_key.is_some() && !config.force_mock {
                            "real"
                        } else {
                            "mock"
                        },
                        "paseo_tools": if secrets.paseo_password.is_some() {
                            "available"
                        } else {
                            "unavailable"
                        }
                    })
                );
            }
            let result = if serve {
                let address = format!("{}:{}", config.listen_host, config.listen_port);
                match tokio::net::TcpListener::bind(&address).await {
                    Ok(listener) => {
                        runtime::serve(
                            config,
                            secrets,
                            runtime::RuntimeDependencies {
                                clock: std::sync::Arc::new(SystemClock::start()),
                                http_client: reqwest::Client::new(),
                                realtime_connector: std::sync::Arc::new(SystemRealtimeConnector),
                            },
                            listener,
                        )
                        .await
                    }
                    Err(error) => Err(format!("listen failed: {error}")),
                }
            } else {
                console::run(&config, secrets)
            };
            match result {
                Ok(()) => ExitCode::SUCCESS,
                Err(error) => {
                    eprintln!("runtime stopped: {error}");
                    ExitCode::from(1)
                }
            }
        }
        _ => {
            eprintln!("usage: paseo-control-plane --version | --serve-stdio | serve | console");
            ExitCode::from(2)
        }
    }
}

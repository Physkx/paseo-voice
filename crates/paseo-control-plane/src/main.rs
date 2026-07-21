#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::env;
use std::process::ExitCode;

use paseo_control_plane::{
    clock::SystemClock, config, console, dictation::HttpDictationCleaner,
    paseo::SystemProcessExecutor, protocol::serve_stdio, realtime::SystemRealtimeConnector,
    runtime, secrets::Secrets, secrets::load_secrets, version_line,
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
            run_operator_command(argument == "serve").await
        }
        _ => {
            eprintln!("usage: paseo-control-plane --version | --serve-stdio | serve | console");
            ExitCode::from(2)
        }
    }
}

async fn run_operator_command(serve: bool) -> ExitCode {
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
    log_runtime_starting(&config, &secrets);
    let result = if serve {
        serve_runtime(config, secrets).await
    } else {
        console::run(&config, &secrets)
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("runtime stopped: {error}");
            ExitCode::from(1)
        }
    }
}

fn log_runtime_starting(config: &config::Config, secrets: &Secrets) {
    if !matches!(config.log_level.as_str(), "debug" | "info") {
        return;
    }
    let realtime_key = runtime::realtime_credential(config, secrets);
    eprintln!(
        "{}",
        serde_json::json!({
            "level": "info",
            "event": "runtime_starting",
            "mode": runtime::realtime_mode(config, realtime_key.as_deref()),
            "realtime_base": config.openai_base_url,
            "realtime_model": config.openai_model,
            "realtime_credentials": if realtime_key.is_some() {
                "available"
            } else {
                "unavailable"
            },
            "paseo_tools": if secrets.paseo_password.is_some() {
                "available"
            } else {
                "unavailable"
            },
            "model_base": config.spark_base_url,
            "model_id": config.spark_model,
            "model_credentials": if secrets.spark_api_key.is_some() {
                "available"
            } else {
                "unavailable"
            },
            "model_credential_source": secrets.model_credential_source.unwrap_or("none")
        })
    );
}

async fn serve_runtime(config: config::Config, secrets: Secrets) -> Result<(), String> {
    let http_client = runtime::build_model_http_client(secrets.spark_api_key.as_deref())?;
    let address = format!("{}:{}", config.listen_host, config.listen_port);
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|error| format!("listen failed: {error}"))?;
    let dictation_cleaner =
        std::sync::Arc::new(HttpDictationCleaner::new(http_client.clone(), &config));
    runtime::serve(
        config,
        secrets,
        runtime::RuntimeDependencies {
            clock: std::sync::Arc::new(SystemClock::start()),
            http_client,
            realtime_connector: std::sync::Arc::new(SystemRealtimeConnector),
            dictation_cleaner,
            process_executor: std::sync::Arc::new(SystemProcessExecutor),
        },
        listener,
    )
    .await
}

#![forbid(unsafe_code)]

use std::env;
use std::process::ExitCode;

use std::collections::HashMap;

use paseo_control_plane::{
    clock::SystemClock, config, console, dictation::HttpDictationCleaner,
    paseo::SystemProcessExecutor, protocol::serve_stdio, realtime::SystemRealtimeConnector,
    runtime, secrets::load_secrets, version_line,
};

#[tokio::main]
#[allow(clippy::too_many_lines)]
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
            let default_voice_credential = config
                .default_voice_profile()
                .credential_ref
                .as_ref()
                .and_then(|id| secrets.api_credentials.get(id));
            let summarisation_credential = config
                .summarisation
                .credential_ref
                .as_ref()
                .and_then(|id| secrets.api_credentials.get(id));
            if matches!(config.log_level.as_str(), "debug" | "info") {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "level": "info",
                        "event": "runtime_starting",
                        "mode": runtime::realtime_mode(
                            &config,
                            default_voice_credential.map(String::as_str),
                        ),
                        "paseo_tools": if secrets.paseo_password.is_some() {
                            "available"
                        } else {
                            "unavailable"
                        },
                        "summarisation_model_id": config.summarisation.model,
                        "summarisation_credentials": if summarisation_credential.is_some() {
                            "available"
                        } else {
                            "unavailable"
                        }
                    })
                );
            }
            let result = if serve {
                match runtime::build_model_http_client(summarisation_credential.map(String::as_str))
                {
                    Ok(http_client) => {
                        let address = format!("{}:{}", config.listen_host, config.listen_port);
                        match tokio::net::TcpListener::bind(&address).await {
                            Ok(listener) => {
                                let mut dictation_cleaners = HashMap::new();
                                for profile in &config.cleanup_profiles {
                                    let credential = profile
                                        .credential_ref
                                        .as_ref()
                                        .and_then(|id| secrets.api_credentials.get(id))
                                        .or_else(|| {
                                            (profile.base_url == "https://api.x.ai/v1")
                                                .then_some(secrets.grok_oauth_token.as_ref())
                                                .flatten()
                                        });
                                    let cleanup_client = match runtime::build_model_http_client(
                                        credential.map(String::as_str),
                                    ) {
                                        Ok(client) => client,
                                        Err(error) => return finish_error(&error),
                                    };
                                    dictation_cleaners.insert(
                                        profile.id.clone(),
                                        std::sync::Arc::new(HttpDictationCleaner::new(
                                            cleanup_client,
                                            profile,
                                        )) as std::sync::Arc<dyn paseo_control_plane::dictation::DictationCleaner>,
                                    );
                                }
                                runtime::serve(
                                    config,
                                    secrets,
                                    runtime::RuntimeDependencies {
                                        clock: std::sync::Arc::new(SystemClock::start()),
                                        http_client,
                                        realtime_connector: std::sync::Arc::new(
                                            SystemRealtimeConnector,
                                        ),
                                        dictation_cleaners,
                                        process_executor: std::sync::Arc::new(
                                            SystemProcessExecutor,
                                        ),
                                    },
                                    listener,
                                )
                                .await
                            }
                            Err(error) => Err(format!("listen failed: {error}")),
                        }
                    }
                    Err(error) => Err(error),
                }
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
        _ => {
            eprintln!("usage: paseo-control-plane --version | --serve-stdio | serve | console");
            ExitCode::from(2)
        }
    }
}

fn finish_error(error: &str) -> ExitCode {
    eprintln!("runtime stopped: {error}");
    ExitCode::from(1)
}

#![forbid(unsafe_code)]

use std::env;
use std::process::ExitCode;

use paseo_control_plane::{protocol::serve_stdio, version_line};

fn main() -> ExitCode {
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
        _ => {
            eprintln!("usage: paseo-control-plane --version | --serve-stdio");
            ExitCode::from(2)
        }
    }
}

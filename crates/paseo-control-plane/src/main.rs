#![forbid(unsafe_code)]

use std::env;
use std::process::ExitCode;

use paseo_control_plane::version_line;

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    match (args.next(), args.next()) {
        (Some(argument), None) if argument == "--version" => {
            println!("{}", version_line());
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("usage: paseo-control-plane --version");
            ExitCode::from(2)
        }
    }
}

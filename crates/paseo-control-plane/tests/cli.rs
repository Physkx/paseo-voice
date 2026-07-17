use std::process::Command;

#[test]
fn version_is_the_only_supported_command() {
    let output = Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"))
        .arg("--version")
        .output()
        .expect("control-plane scaffold should run");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("version output should be UTF-8"),
        format!("paseo-control-plane {}\n", env!("CARGO_PKG_VERSION")),
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn unsupported_arguments_fail_without_starting_a_runtime() {
    let output = Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"))
        .arg("--help")
        .output()
        .expect("control-plane scaffold should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).expect("usage output should be UTF-8"),
        "usage: paseo-control-plane --version\n",
    );
}

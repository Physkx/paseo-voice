use std::{
    io::Write,
    process::{Command, Stdio},
};

use paseo_control_plane::protocol::frame_payload;

#[test]
fn version_reports_the_binary_version() {
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
        "usage: paseo-control-plane --version | --serve-stdio | serve\n",
    );
}

#[test]
fn stdio_server_handles_a_frame_and_shuts_down_on_eof() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_paseo-control-plane"))
        .arg("--serve-stdio")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start control-plane server");
    let request = frame_payload(br#"{"version":1,"request_id":"health","op":"health"}"#)
        .expect("frame health request");
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(&request)
        .expect("write health request");

    let output = child
        .wait_with_output()
        .expect("wait for clean EOF shutdown");
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let length = u32::from_be_bytes(output.stdout[..4].try_into().expect("response prefix"));
    assert_eq!(
        usize::try_from(length).expect("response length") + 4,
        output.stdout.len()
    );
    let response: serde_json::Value =
        serde_json::from_slice(&output.stdout[4..]).expect("JSON response");
    assert_eq!(response["result"]["status"], "healthy");
}

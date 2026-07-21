#![cfg(unix)]

use std::{
    collections::HashMap,
    os::unix::fs::PermissionsExt as _,
    path::PathBuf,
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use nix::sys::signal::killpg;
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use paseo_control_plane::paseo::{ProcessExecutor, SystemProcessExecutor};

const PROCESS_OUTPUT_CAP: usize = 8 * 1024 * 1024;
const TEST_PROCESS_DEADLINE: Duration = Duration::from_millis(500);
const BOUNDED_RETURN_ALLOWANCE: Duration = Duration::from_secs(5);

fn shell_fixture(source: &str) -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::tempdir().expect("temporary fixture directory");
    let path = directory.path().join("fixture.sh");
    std::fs::write(&path, source).expect("write shell fixture");
    let mut permissions = std::fs::metadata(&path)
        .expect("shell fixture metadata")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&path, permissions).expect("make shell fixture executable");
    (directory, path)
}

#[cfg(target_os = "linux")]
fn process_is_running(pid: i32) -> bool {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    stat.rsplit_once(") ")
        .and_then(|(_, fields)| fields.chars().next())
        .is_some_and(|state| state != 'Z' && state != 'X')
}

#[cfg(target_os = "linux")]
fn current_process_has_fd_target(target: &std::path::Path) -> bool {
    std::fs::read_dir("/proc/self/fd")
        .expect("read current process descriptors")
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .any(|candidate| candidate == target)
}

#[cfg(target_os = "linux")]
#[test]
fn returns_by_a_hard_bound_when_an_exited_child_leaves_inherited_pipes_open() {
    let (directory, fixture) =
        shell_fixture("#!/bin/sh\n/bin/sleep 10 &\n/usr/bin/printf '%s' \"$!\" > \"$1\"\nexit 0\n");
    let pid_path = directory.path().join("descendant.pid");
    let args = [pid_path.to_string_lossy().into_owned()];
    let started = Instant::now();

    let output = SystemProcessExecutor.execute(
        fixture.to_str().expect("UTF-8 fixture path"),
        &args,
        &HashMap::new(),
        TEST_PROCESS_DEADLINE,
    );

    let descendant_pid = std::fs::read_to_string(pid_path)
        .expect("read descendant PID")
        .parse::<i32>()
        .expect("parse descendant PID");
    let descendant_survived = process_is_running(descendant_pid);
    let _ = kill(Pid::from_raw(descendant_pid), Signal::SIGKILL);
    assert!(
        started.elapsed() < BOUNDED_RETURN_ALLOWANCE,
        "executor exceeded its conservative bounded-return allowance"
    );
    assert!(!output.spawn_failed);
    assert_eq!(output.exit_code, Some(0));
    assert!(output.timed_out);
    assert!(
        descendant_survived,
        "executor signalled a process group after reaping its leader"
    );
}

#[test]
fn returns_by_a_hard_bound_when_a_timed_out_child_has_a_descendant() {
    let (_directory, fixture) = shell_fixture("#!/bin/sh\n/bin/sleep 10 &\n/bin/sleep 10\n");
    let started = Instant::now();

    let output = SystemProcessExecutor.execute(
        fixture.to_str().expect("UTF-8 fixture path"),
        &[],
        &HashMap::new(),
        TEST_PROCESS_DEADLINE,
    );

    assert!(
        started.elapsed() < BOUNDED_RETURN_ALLOWANCE,
        "executor exceeded its conservative bounded-return allowance"
    );
    assert!(!output.spawn_failed);
    assert_eq!(output.exit_code, None);
    assert!(output.timed_out);
}

#[cfg(target_os = "linux")]
#[test]
fn returns_by_a_hard_bound_when_a_descendant_escapes_the_owned_process_group() {
    let (_fixture_directory, fixture) = shell_fixture(
        "#!/bin/sh\n/usr/bin/setsid \"$2\" \"$1\" &\nwhile [ ! -s \"$1\" ]; do /bin/sleep 0.01; done\nexit 0\n",
    );
    let (escaped_directory, escaped_fixture) =
        shell_fixture("#!/bin/sh\n/usr/bin/printf '%s' \"$$\" > \"$1\"\n/bin/sleep 10\n");
    let pid_path = escaped_directory.path().join("escaped.pid");
    let args = [
        pid_path.to_string_lossy().into_owned(),
        escaped_fixture.to_string_lossy().into_owned(),
    ];
    let started = Instant::now();

    let output = SystemProcessExecutor.execute(
        fixture.to_str().expect("UTF-8 fixture path"),
        &args,
        &HashMap::new(),
        TEST_PROCESS_DEADLINE,
    );

    let escaped_pid = std::fs::read_to_string(pid_path)
        .expect("read escaped fixture PID")
        .parse::<i32>()
        .expect("parse escaped fixture PID");
    let escaped_survived = process_is_running(escaped_pid);
    let escaped_stdout = std::fs::read_link(format!("/proc/{escaped_pid}/fd/1"))
        .expect("read escaped stdout descriptor");
    let escaped_stderr = std::fs::read_link(format!("/proc/{escaped_pid}/fd/2"))
        .expect("read escaped stderr descriptor");
    let stdout_reader_retained = current_process_has_fd_target(&escaped_stdout);
    let stderr_reader_retained = current_process_has_fd_target(&escaped_stderr);
    let _ = killpg(Pid::from_raw(escaped_pid), Signal::SIGKILL);
    assert!(
        started.elapsed() < BOUNDED_RETURN_ALLOWANCE,
        "executor exceeded its conservative bounded-return allowance"
    );
    assert_eq!(output.exit_code, Some(0));
    assert!(output.timed_out);
    assert!(!output.spawn_failed);
    assert!(
        escaped_survived,
        "escaped descendant was unexpectedly killed"
    );
    assert!(!stdout_reader_retained, "stdout reader retained its pipe");
    assert!(!stderr_reader_retained, "stderr reader retained its pipe");
}

#[test]
fn captures_normal_output_with_exact_arguments_and_environment() {
    let (_directory, fixture) = shell_fixture(
        "#!/bin/sh\nif [ \"${HOME+x}\" = x ]; then exit 99; fi\n/usr/bin/printf 'out:%s:%s' \"$1\" \"$ONLY_VALUE\"\n/usr/bin/printf 'err:%s' \"$2\" >&2\n",
    );
    let args = [
        "argument with spaces;$(ignored)".to_owned(),
        "error value".to_owned(),
    ];
    let env = HashMap::from([("ONLY_VALUE".to_owned(), " exact environment ".to_owned())]);

    let output = SystemProcessExecutor.execute(
        fixture.to_str().expect("UTF-8 fixture path"),
        &args,
        &env,
        Duration::from_secs(1),
    );

    assert_eq!(output.exit_code, Some(0));
    assert_eq!(
        output.stdout,
        "out:argument with spaces;$(ignored): exact environment "
    );
    assert_eq!(output.stderr, "err:error value");
    assert!(!output.timed_out);
    assert!(!output.spawn_failed);
}

#[test]
fn preserves_nonzero_exit_and_bounded_output() {
    let (_directory, fixture) = shell_fixture(
        "#!/bin/sh\n/usr/bin/printf 'nonzero stdout'\n/usr/bin/printf 'nonzero stderr' >&2\nexit 23\n",
    );

    let output = SystemProcessExecutor.execute(
        fixture.to_str().expect("UTF-8 fixture path"),
        &[],
        &HashMap::new(),
        Duration::from_secs(1),
    );

    assert_eq!(output.exit_code, Some(23));
    assert_eq!(output.stdout, "nonzero stdout");
    assert_eq!(output.stderr, "nonzero stderr");
    assert!(!output.timed_out);
    assert!(!output.spawn_failed);
}

#[test]
fn caps_each_output_stream_and_marks_any_extra_byte_uncertain() {
    let (directory, fixture) = shell_fixture("#!/bin/sh\n/bin/cat \"$1\"\n/bin/cat \"$1\" >&2\n");
    let payload = directory.path().join("payload");
    std::fs::write(&payload, vec![b'x'; PROCESS_OUTPUT_CAP + 64 * 1024])
        .expect("write oversized fixture payload");
    let args = [payload.to_string_lossy().into_owned()];

    let output = SystemProcessExecutor.execute(
        fixture.to_str().expect("UTF-8 fixture path"),
        &args,
        &HashMap::new(),
        Duration::from_secs(5),
    );

    assert_eq!(output.stdout.len(), PROCESS_OUTPUT_CAP);
    assert_eq!(output.stderr.len(), PROCESS_OUTPUT_CAP);
    assert!(output.timed_out);
    assert!(!output.spawn_failed);
}

#[test]
fn accepts_exactly_eight_mibibytes_after_the_overflow_probe_reaches_eof() {
    let directory = tempfile::tempdir().expect("temporary payload directory");
    let payload = directory.path().join("exact-payload");
    std::fs::write(&payload, vec![b'x'; PROCESS_OUTPUT_CAP])
        .expect("write exact-sized fixture payload");
    let args = [payload.to_string_lossy().into_owned()];

    let output =
        SystemProcessExecutor.execute("/bin/cat", &args, &HashMap::new(), Duration::from_secs(5));

    assert_eq!(output.exit_code, Some(0));
    assert_eq!(output.stdout.len(), PROCESS_OUTPUT_CAP);
    assert!(output.stderr.is_empty());
    assert!(!output.timed_out);
    assert!(!output.spawn_failed);
}

#[test]
fn process_diagnostics_do_not_expose_arguments_environment_or_output() {
    const SECRET: &str = "process-secret-must-not-appear-in-diagnostics";
    let (_directory, fixture) = shell_fixture(
        "#!/bin/sh\n/usr/bin/printf '%s' \"$1\"\n/usr/bin/printf '%s' \"$ONLY_SECRET\" >&2\n",
    );
    let args = [SECRET.to_owned()];
    let env = HashMap::from([("ONLY_SECRET".to_owned(), SECRET.to_owned())]);

    let output = SystemProcessExecutor.execute(
        fixture.to_str().expect("UTF-8 fixture path"),
        &args,
        &env,
        Duration::from_secs(1),
    );

    assert_eq!(output.stdout, SECRET);
    assert_eq!(output.stderr, SECRET);
    assert!(!format!("{output:?}").contains(SECRET));
}

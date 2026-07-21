//! Direct, shell-free Paseo process adapter.

use std::{
    collections::HashMap,
    fmt,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, OnceLock, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
#[cfg(not(unix))]
use std::sync::mpsc::{Receiver, RecvTimeoutError};

#[cfg(unix)]
use nix::{
    errno::Errno,
    fcntl::{FcntlArg, OFlag, fcntl},
    poll::{PollFd, PollFlags, PollTimeout, poll},
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use serde_json::Value;
use wait_timeout::ChildExt as _;

use paseo_safety_core::DispatchAuthorization;

const MAX_PROCESS_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
const PROCESS_TERMINATION_GRACE: Duration = Duration::from_millis(100);
const PROCESS_READ_CHUNK_BYTES: usize = 16 * 1024;
#[cfg(unix)]
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(20);
const PROCESS_REAPER_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Captured output from a directly executed child process.
#[derive(Clone, Eq, PartialEq)]
pub struct ProcessOutput {
    /// Process exit code, or none if no exit status was obtained.
    pub exit_code: Option<i32>,
    /// Bounded standard output.
    pub stdout: String,
    /// Bounded standard error.
    pub stderr: String,
    /// Whether the post-spawn deadline, output capture, or output bound became uncertain.
    pub timed_out: bool,
    /// Whether spawning failed before the receiver could observe a request.
    pub spawn_failed: bool,
}

impl fmt::Debug for ProcessOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessOutput")
            .field("exit_code", &self.exit_code)
            .field("stdout_bytes", &self.stdout.len())
            .field("stderr_bytes", &self.stderr.len())
            .field("timed_out", &self.timed_out)
            .field("spawn_failed", &self.spawn_failed)
            .finish()
    }
}

impl ProcessOutput {
    fn is_certain_success(&self) -> bool {
        !self.spawn_failed && !self.timed_out && self.exit_code == Some(0)
    }
}

/// Injected direct process boundary. Implementations must never invoke a shell.
pub trait ProcessExecutor: Send + Sync {
    /// Execute one program with exact arguments and environment.
    fn execute(
        &self,
        program: &str,
        args: &[String],
        env: &HashMap<String, String>,
        timeout: Duration,
    ) -> ProcessOutput;
}

impl<T: ProcessExecutor + ?Sized> ProcessExecutor for Arc<T> {
    fn execute(
        &self,
        program: &str,
        args: &[String],
        env: &HashMap<String, String>,
        timeout: Duration,
    ) -> ProcessOutput {
        self.as_ref().execute(program, args, env, timeout)
    }
}

/// Operating-system implementation using direct process execution.
#[derive(Clone, Copy)]
pub struct SystemProcessExecutor;

impl ProcessExecutor for SystemProcessExecutor {
    fn execute(
        &self,
        program: &str,
        args: &[String],
        env: &HashMap<String, String>,
        timeout: Duration,
    ) -> ProcessOutput {
        let Some(reaper) = child_reaper() else {
            return spawn_failed_output();
        };
        let mut command = Command::new(program);
        command
            .args(args)
            .env_clear()
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        // The unreaped child owns the only process group this executor may signal.
        command.process_group(0);

        let spawned = command.spawn();
        let Ok(child) = spawned else {
            return spawn_failed_output();
        };
        let started = Instant::now();
        let deadline = started.checked_add(timeout).unwrap_or(started);
        #[cfg(unix)]
        {
            execute_unix(child, deadline, &reaper)
        }
        #[cfg(not(unix))]
        {
            execute_non_unix(child, deadline, &reaper)
        }
    }
}

fn spawn_failed_output() -> ProcessOutput {
    ProcessOutput {
        exit_code: None,
        stdout: String::new(),
        stderr: String::new(),
        timed_out: false,
        spawn_failed: true,
    }
}

#[cfg(unix)]
fn execute_unix(
    mut child: Child,
    deadline: Instant,
    reaper: &mpsc::Sender<Child>,
) -> ProcessOutput {
    let stdout_reader =
        UnixOutputReader::spawn(child.stdout.take(), deadline, "paseo-stdout-reader");
    let stderr_reader =
        UnixOutputReader::spawn(child.stderr.take(), deadline, "paseo-stderr-reader");
    let mut status = None;
    let mut timed_out = stdout_reader.is_none() || stderr_reader.is_none();
    let mut unreaped = false;
    let mut reap_without_signal = false;

    if let Ok(Some(value)) = child.wait_timeout(remaining(deadline)) {
        status = Some(value);
        timed_out |= Instant::now() >= deadline;
    } else {
        timed_out = true;
        match child.try_wait() {
            Ok(Some(value)) => status = Some(value),
            Ok(None) => unreaped = true,
            Err(_) => reap_without_signal = true,
        }
    }

    if unreaped {
        terminate_unreaped_unix_child(&mut child);
        status = reap_after_termination(child, PROCESS_TERMINATION_GRACE, reaper);
    } else if reap_without_signal {
        reap_in_background(child, reaper);
    }

    let stdout = finish_unix_reader(stdout_reader);
    let stderr = finish_unix_reader(stderr_reader);
    timed_out |= reader_uncertain(stdout.as_ref()) || reader_uncertain(stderr.as_ref());

    ProcessOutput {
        exit_code: status.and_then(|value| value.code()),
        stdout: stdout.map_or_else(String::new, |value| value.text),
        stderr: stderr.map_or_else(String::new, |value| value.text),
        timed_out,
        spawn_failed: false,
    }
}

#[cfg(unix)]
struct UnixOutputReader {
    thread: JoinHandle<ReaderOutput>,
}

#[cfg(unix)]
impl UnixOutputReader {
    fn spawn(
        pipe: Option<impl std::io::Read + std::os::fd::AsFd + Send + 'static>,
        deadline: Instant,
        name: &str,
    ) -> Option<Self> {
        let pipe = pipe?;
        set_nonblocking(&pipe)?;
        let thread = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || read_bounded_until(pipe, deadline))
            .ok()?;
        Some(Self { thread })
    }
}

#[cfg(unix)]
fn set_nonblocking(pipe: &impl std::os::fd::AsFd) -> Option<()> {
    let flags = fcntl(pipe, FcntlArg::F_GETFL).ok()?;
    let flags = OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK;
    fcntl(pipe, FcntlArg::F_SETFL(flags)).ok()?;
    Some(())
}

#[cfg(unix)]
fn read_bounded_until(
    mut pipe: impl std::io::Read + std::os::fd::AsFd,
    deadline: Instant,
) -> ReaderOutput {
    let mut bytes = Vec::new();
    let mut read_failed = false;
    let mut deadline_reached = false;
    let mut overflowed = false;
    let mut chunk = [0_u8; PROCESS_READ_CHUNK_BYTES];

    loop {
        let wait = remaining(deadline);
        if wait.is_zero() {
            deadline_reached = true;
            break;
        }
        let timeout =
            PollTimeout::try_from(wait.min(PROCESS_POLL_INTERVAL)).unwrap_or(PollTimeout::MAX);
        let revents = {
            let mut descriptors = [PollFd::new(pipe.as_fd(), PollFlags::POLLIN)];
            match poll(&mut descriptors, timeout) {
                Ok(0) | Err(Errno::EINTR) => continue,
                Ok(_) => descriptors[0].revents(),
                Err(_) => {
                    read_failed = true;
                    break;
                }
            }
        };
        let Some(revents) = revents else {
            read_failed = true;
            break;
        };
        if revents.contains(PollFlags::POLLNVAL) {
            read_failed = true;
            break;
        }
        if !revents.intersects(PollFlags::POLLIN | PollFlags::POLLHUP | PollFlags::POLLERR) {
            continue;
        }

        match pipe.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                let retained = read.min(MAX_PROCESS_OUTPUT_BYTES - bytes.len());
                bytes.extend_from_slice(&chunk[..retained]);
                if retained < read {
                    overflowed = true;
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) => {}
            Err(_) => {
                read_failed = true;
                break;
            }
        }
    }

    reader_output(bytes, read_failed, deadline_reached, overflowed)
}

#[cfg(unix)]
fn finish_unix_reader(reader: Option<UnixOutputReader>) -> Option<ReaderOutput> {
    reader?.thread.join().ok()
}

#[cfg(not(unix))]
fn execute_non_unix(
    mut child: Child,
    deadline: Instant,
    reaper: &mpsc::Sender<Child>,
) -> ProcessOutput {
    let stdout_reader = OutputReader::spawn(child.stdout.take(), "paseo-stdout-reader");
    let stderr_reader = OutputReader::spawn(child.stderr.take(), "paseo-stderr-reader");
    let mut stdout = None;
    let mut stderr = None;
    let mut status = None;
    let mut timed_out = stdout_reader.is_none() || stderr_reader.is_none();
    let mut unreaped = false;
    let mut reap_without_signal = false;

    if let Ok(Some(value)) = child.wait_timeout(remaining(deadline)) {
        status = Some(value);
        timed_out |= Instant::now() >= deadline;
    } else {
        timed_out = true;
        match child.try_wait() {
            Ok(Some(value)) => status = Some(value),
            Ok(None) => unreaped = true,
            Err(_) => reap_without_signal = true,
        }
    }
    let finish_deadline = if unreaped {
        let _ = child.kill();
        let drain_started = Instant::now();
        let drain_deadline = drain_started
            .checked_add(PROCESS_TERMINATION_GRACE)
            .unwrap_or(drain_started);
        status = reap_after_termination(child, remaining(drain_deadline), reaper);
        let _ = receive_until(stdout_reader.as_ref(), &mut stdout, drain_deadline);
        let _ = receive_until(stderr_reader.as_ref(), &mut stderr, drain_deadline);
        drain_deadline
    } else {
        if reap_without_signal {
            reap_in_background(child, reaper);
        }
        let _ = receive_until(stdout_reader.as_ref(), &mut stdout, deadline);
        let _ = receive_until(stderr_reader.as_ref(), &mut stderr, deadline);
        deadline
    };
    timed_out |= reader_uncertain(stdout.as_ref()) || reader_uncertain(stderr.as_ref());
    finish_reader(stdout_reader, finish_deadline);
    finish_reader(stderr_reader, finish_deadline);

    ProcessOutput {
        exit_code: status.and_then(|value| value.code()),
        stdout: stdout.map_or_else(String::new, |value| value.text),
        stderr: stderr.map_or_else(String::new, |value| value.text),
        timed_out,
        spawn_failed: false,
    }
}

#[cfg(not(unix))]
struct OutputReader {
    receiver: Receiver<ReaderOutput>,
    thread: JoinHandle<()>,
}

#[cfg(not(unix))]
impl OutputReader {
    fn spawn(pipe: Option<impl std::io::Read + Send + 'static>, name: &str) -> Option<Self> {
        let pipe = pipe?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let _ = sender.send(read_bounded(pipe));
            })
            .ok()?;
        Some(Self { receiver, thread })
    }
}

struct ReaderOutput {
    text: String,
    read_failed: bool,
    deadline_reached: bool,
    overflowed: bool,
}

fn reader_output(
    bytes: Vec<u8>,
    mut read_failed: bool,
    deadline_reached: bool,
    overflowed: bool,
) -> ReaderOutput {
    let text = String::from_utf8(bytes).unwrap_or_else(|_| {
        read_failed = true;
        String::new()
    });
    ReaderOutput {
        text,
        read_failed,
        deadline_reached,
        overflowed,
    }
}

#[cfg(not(unix))]
fn read_bounded(mut pipe: impl std::io::Read) -> ReaderOutput {
    let mut bytes = Vec::new();
    let mut read_failed = false;
    let mut overflowed = false;
    let mut chunk = [0_u8; PROCESS_READ_CHUNK_BYTES];
    loop {
        match pipe.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                let retained = read.min(MAX_PROCESS_OUTPUT_BYTES - bytes.len());
                bytes.extend_from_slice(&chunk[..retained]);
                if retained < read {
                    overflowed = true;
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => {
                read_failed = true;
                break;
            }
        }
    }
    reader_output(bytes, read_failed, false, overflowed)
}

#[cfg(not(unix))]
fn receive_until(
    reader: Option<&OutputReader>,
    output: &mut Option<ReaderOutput>,
    deadline: Instant,
) -> bool {
    if output.is_some() {
        return false;
    }
    let Some(reader) = reader else {
        return true;
    };
    let wait = remaining(deadline);
    if wait.is_zero() {
        return true;
    }
    match reader.receiver.recv_timeout(wait) {
        Ok(value) => {
            *output = Some(value);
            false
        }
        Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => true,
    }
}

fn reader_uncertain(output: Option<&ReaderOutput>) -> bool {
    output.is_none_or(|reader_output| {
        reader_output.read_failed || reader_output.deadline_reached || reader_output.overflowed
    })
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

#[cfg(not(unix))]
fn finish_reader(reader: Option<OutputReader>, deadline: Instant) {
    let Some(reader) = reader else {
        return;
    };
    while !reader.thread.is_finished() {
        let wait = remaining(deadline);
        if wait.is_zero() {
            // Dropping the unfinished handle detaches only the still-capped pipe reader.
            return;
        }
        thread::park_timeout(wait.min(Duration::from_millis(1)));
    }
    let _ = reader.thread.join();
}

#[cfg(unix)]
fn terminate_unreaped_unix_child(child: &mut Child) {
    if let Ok(raw_pid) = i32::try_from(child.id()) {
        let _ = killpg(Pid::from_raw(raw_pid), Signal::SIGKILL);
    }
    let _ = child.kill();
}

fn child_reaper() -> Option<mpsc::Sender<Child>> {
    static REAPER: OnceLock<Option<mpsc::Sender<Child>>> = OnceLock::new();
    REAPER
        .get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<Child>();
            thread::Builder::new()
                .name("paseo-child-reaper".to_owned())
                .spawn(move || run_child_reaper(&receiver))
                .ok()
                .map(|_| sender)
        })
        .clone()
}

fn run_child_reaper(receiver: &mpsc::Receiver<Child>) {
    let mut children = Vec::<Child>::new();
    let mut disconnected = false;
    loop {
        let mut index = 0;
        while index < children.len() {
            match children[index].try_wait() {
                Ok(Some(_)) => {
                    let mut child = children.swap_remove(index);
                    let _ = child.wait();
                }
                Ok(None) | Err(_) => index += 1,
            }
        }

        if disconnected {
            if children.is_empty() {
                return;
            }
            thread::sleep(PROCESS_REAPER_POLL_INTERVAL);
            continue;
        }

        match receiver.recv_timeout(PROCESS_REAPER_POLL_INTERVAL) {
            Ok(child) => children.push(child),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => disconnected = true,
        }
    }
}

fn reap_after_termination(
    mut child: Child,
    grace: Duration,
    reaper: &mpsc::Sender<Child>,
) -> Option<ExitStatus> {
    if let Ok(Some(status)) = child.wait_timeout(grace) {
        Some(status)
    } else {
        reap_in_background(child, reaper);
        None
    }
}

fn reap_in_background(child: Child, reaper: &mpsc::Sender<Child>) {
    assert!(
        reaper.send(child).is_ok(),
        "process reaper exited unexpectedly"
    );
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use nix::{
        errno::Errno,
        sys::signal::{Signal, kill},
        unistd::Pid,
    };

    fn wait_until_reaped(pid: i32, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if kill(Pid::from_raw(pid), None) == Err(Errno::ESRCH) {
                return true;
            }
            thread::sleep(Duration::from_millis(10));
        }
        false
    }

    #[test]
    fn delayed_child_moves_to_the_background_reaper_after_zero_grace() {
        let child = Command::new("/bin/sleep")
            .arg("0.05")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn delayed reaper fixture");
        let pid = i32::try_from(child.id()).expect("fixture PID fits i32");
        let reaper = child_reaper().expect("background child reaper");

        assert!(reap_after_termination(child, Duration::ZERO, &reaper).is_none());

        assert!(
            wait_until_reaped(pid, Duration::from_secs(2)),
            "background reaper did not wait for the delayed child"
        );
    }

    #[test]
    fn background_reaper_reaps_a_short_child_behind_a_long_child() {
        let long = Command::new("/bin/sleep")
            .arg("10")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn long reaper fixture");
        let long_pid = i32::try_from(long.id()).expect("long fixture PID fits i32");
        let short = Command::new("/bin/sleep")
            .arg("0.05")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn short reaper fixture");
        let short_pid = i32::try_from(short.id()).expect("short fixture PID fits i32");
        let reaper = child_reaper().expect("background child reaper");
        reap_in_background(long, &reaper);
        reap_in_background(short, &reaper);

        let short_reaped_first = wait_until_reaped(short_pid, Duration::from_millis(500));
        let long_still_running = kill(Pid::from_raw(long_pid), None).is_ok();
        let _ = kill(Pid::from_raw(long_pid), Some(Signal::SIGKILL));
        let long_reaped = wait_until_reaped(long_pid, Duration::from_secs(2));
        let short_eventually_reaped = wait_until_reaped(short_pid, Duration::from_secs(2));

        assert!(
            short_reaped_first,
            "short child was starved behind long child"
        );
        assert!(
            long_still_running,
            "long child exited before fairness check"
        );
        assert!(long_reaped, "long child was not reaped after cleanup");
        assert!(short_eventually_reaped, "short child was not safely reaped");
    }

    #[test]
    fn disconnected_reaper_drains_owned_children_before_exit() {
        let child = Command::new("/bin/sleep")
            .arg("0.05")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn disconnect reaper fixture");
        let pid = i32::try_from(child.id()).expect("fixture PID fits i32");
        let (sender, receiver) = mpsc::channel();
        let reaper = thread::Builder::new()
            .name("paseo-child-reaper-test".to_owned())
            .spawn(move || run_child_reaper(&receiver))
            .expect("spawn isolated reaper");
        sender.send(child).expect("queue child");
        drop(sender);

        reaper.join().expect("disconnected reaper exits");
        assert_eq!(kill(Pid::from_raw(pid), None), Err(Errno::ESRCH));
    }
}

/// Conservative result of a Paseo write attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteResult {
    /// A receiver acknowledgement contained a validated message identity.
    Delivered { receiver_message_id: String },
    /// No child process existed for the attempted action.
    Rejected,
    /// Dispatch may have occurred but no authoritative receipt was validated.
    OutcomeUnknown,
}

/// Conservative result of creating a new Paseo session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunResult {
    /// Paseo returned a validated new agent identity.
    Created { agent_id: String },
    /// No child process existed for the attempted action.
    Rejected,
    /// Creation may have occurred but no valid agent identity was returned.
    OutcomeUnknown,
}

/// Opaque confirmed new-run arguments. Only the control-plane gate can construct this value.
pub struct RunAuthorization {
    prompt: String,
    provider: String,
    cwd: String,
}

impl RunAuthorization {
    pub(crate) fn new(prompt: String, provider: String, cwd: String) -> Self {
        Self {
            prompt,
            provider,
            cwd,
        }
    }
}

/// Paseo adapter that holds the only write credential in memory.
pub struct PaseoAdapter<E> {
    executor: E,
    binary: String,
    password: String,
    host: Option<String>,
}

impl<E: ProcessExecutor> PaseoAdapter<E> {
    /// Construct a credential-owning adapter.
    #[must_use]
    pub fn new(executor: E, binary: String, password: String, host: Option<String>) -> Self {
        Self {
            executor,
            binary,
            password,
            host,
        }
    }

    /// Send the exact confirmed response to its provenance-derived thread.
    #[must_use]
    pub fn send_message(&self, authorization: &DispatchAuthorization) -> WriteResult {
        let args = [
            "send".to_owned(),
            authorization.destination_thread_id().as_str().to_owned(),
            "--prompt".to_owned(),
            authorization.response().as_str().to_owned(),
            "--no-wait".to_owned(),
            "--json".to_owned(),
        ];
        let env = self.child_environment();
        classify_write(
            &self
                .executor
                .execute(&self.binary, &args, &env, Duration::from_secs(20)),
        )
    }

    /// List sessions as strict JSON rows.
    #[must_use]
    pub fn list_sessions(&self, include_archived: bool) -> Option<Vec<Value>> {
        let output = if include_archived {
            self.invoke(&["ls", "-a", "-g", "--json"], Duration::from_secs(20))
        } else {
            self.invoke(&["ls", "-g", "--json"], Duration::from_secs(20))
        };
        if !output.is_certain_success() {
            return None;
        }
        serde_json::from_str::<Vec<Value>>(&output.stdout).ok()
    }

    /// Read recent assistant output without parsing content as JSON.
    #[must_use]
    pub fn read_log_text(&self, thread_id: &str, tail: usize, filter_text: bool) -> Option<String> {
        let tail = tail.min(200).to_string();
        let output = if filter_text {
            self.invoke(
                &["logs", thread_id, "--tail", &tail, "--filter", "text"],
                Duration::from_secs(20),
            )
        } else {
            self.invoke(
                &["logs", thread_id, "--tail", &tail],
                Duration::from_secs(20),
            )
        };
        output
            .is_certain_success()
            .then(|| output.stdout.trim().to_owned())
    }

    /// List pending permission requests for narration only.
    #[must_use]
    pub fn list_pending_permissions(&self) -> Option<Vec<Value>> {
        let output = self.invoke(&["permit", "ls", "--json"], Duration::from_secs(20));
        if !output.is_certain_success() {
            return None;
        }
        serde_json::from_str::<Vec<Value>>(&output.stdout).ok()
    }

    /// Check that an exact provider/model pair is advertised by the daemon.
    #[must_use]
    pub fn provider_model_available(&self, provider_model: &str) -> bool {
        let Some((provider, model)) = provider_model.split_once('/') else {
            return false;
        };
        if provider.is_empty() || model.is_empty() || model.contains('/') {
            return false;
        }
        let output = self.invoke(
            &["provider", "models", provider, "--json"],
            Duration::from_secs(20),
        );
        if !output.is_certain_success() {
            return false;
        }
        serde_json::from_str::<Vec<Value>>(&output.stdout)
            .ok()
            .is_some_and(|models| {
                models.iter().any(|entry| {
                    matches!(entry.get("id").and_then(Value::as_str), Some(id) if id == model || id == provider_model)
                })
            })
    }

    /// Start a confirmed detached run with exact stored arguments.
    #[must_use]
    pub fn start_run(&self, authorization: &RunAuthorization) -> RunResult {
        let mut args = vec![
            "run".to_owned(),
            authorization.prompt.clone(),
            "--detach".to_owned(),
        ];
        args.extend([
            "--provider".to_owned(),
            authorization.provider.clone(),
            "--cwd".to_owned(),
            authorization.cwd.clone(),
        ]);
        args.push("--json".to_owned());
        classify_run(&self.executor.execute(
            &self.binary,
            &args,
            &self.child_environment(),
            Duration::from_mins(1),
        ))
    }

    fn invoke(&self, args: &[&str], timeout: Duration) -> ProcessOutput {
        let args = args.iter().map(ToString::to_string).collect::<Vec<_>>();
        let env = self.child_environment();
        self.executor.execute(&self.binary, &args, &env, timeout)
    }

    fn child_environment(&self) -> HashMap<String, String> {
        let snapshot: HashMap<String, String> = std::env::vars().collect();
        let mut env = crate::secrets::passthrough_env(&snapshot);
        env.insert("PASEO_PASSWORD".to_owned(), self.password.clone());
        if let Some(host) = &self.host {
            env.insert("PASEO_HOST".to_owned(), host.clone());
        }
        env
    }
}

fn classify_write(output: &ProcessOutput) -> WriteResult {
    if output.spawn_failed {
        return WriteResult::Rejected;
    }
    if !output.is_certain_success() {
        return WriteResult::OutcomeUnknown;
    }
    let Ok(value) = serde_json::from_str::<Value>(&output.stdout) else {
        return WriteResult::OutcomeUnknown;
    };
    let Some(identifier) = value.get("messageId").and_then(Value::as_str) else {
        return WriteResult::OutcomeUnknown;
    };
    if paseo_safety_core::Identifier::new(identifier).is_err() {
        return WriteResult::OutcomeUnknown;
    }
    WriteResult::Delivered {
        receiver_message_id: identifier.to_owned(),
    }
}

fn classify_run(output: &ProcessOutput) -> RunResult {
    if output.spawn_failed {
        return RunResult::Rejected;
    }
    if !output.is_certain_success() {
        return RunResult::OutcomeUnknown;
    }
    let Ok(value) = serde_json::from_str::<Value>(&output.stdout) else {
        return RunResult::OutcomeUnknown;
    };
    let Some(agent_id) = value.get("agentId").and_then(Value::as_str) else {
        return RunResult::OutcomeUnknown;
    };
    if paseo_safety_core::Identifier::new(agent_id).is_err() {
        return RunResult::OutcomeUnknown;
    }
    RunResult::Created {
        agent_id: agent_id.to_owned(),
    }
}

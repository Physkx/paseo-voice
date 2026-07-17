//! Direct, shell-free Paseo process adapter.

use std::{
    collections::HashMap,
    io::Read as _,
    process::{Command, Stdio},
    time::Duration,
};

use serde_json::Value;
use wait_timeout::ChildExt as _;

const MAX_PROCESS_OUTPUT_BYTES: u64 = 8 * 1024 * 1024;

/// Captured output from a directly executed child process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessOutput {
    /// Process exit code, or none if no exit status was obtained.
    pub exit_code: Option<i32>,
    /// Bounded standard output.
    pub stdout: String,
    /// Bounded standard error.
    pub stderr: String,
    /// Whether the child exceeded its deadline after spawn.
    pub timed_out: bool,
    /// Whether spawning failed before the receiver could observe a request.
    pub spawn_failed: bool,
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
        let spawned = Command::new(program)
            .args(args)
            .env_clear()
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();
        let Ok(mut child) = spawned else {
            return ProcessOutput {
                exit_code: None,
                stdout: String::new(),
                stderr: String::new(),
                timed_out: false,
                spawn_failed: true,
            };
        };
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let stdout_reader = std::thread::spawn(move || read_bounded(stdout));
        let stderr_reader = std::thread::spawn(move || read_bounded(stderr));
        let status = child.wait_timeout(timeout).ok().flatten();
        let timed_out = status.is_none();
        if timed_out {
            let _ = child.kill();
        }
        let final_status = status.or_else(|| child.wait().ok());
        ProcessOutput {
            exit_code: final_status.and_then(|value| value.code()),
            stdout: stdout_reader.join().unwrap_or_default(),
            stderr: stderr_reader.join().unwrap_or_default(),
            timed_out,
            spawn_failed: false,
        }
    }
}

fn read_bounded(pipe: Option<impl std::io::Read>) -> String {
    let mut bytes = Vec::new();
    if let Some(pipe) = pipe {
        let _ = pipe.take(MAX_PROCESS_OUTPUT_BYTES).read_to_end(&mut bytes);
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Conservative result of a Paseo write attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteResult {
    /// A receiver acknowledgement contained a validated message identity.
    Delivered { receiver_message_id: String },
    /// The process could not start or Paseo authoritatively rejected the command.
    Rejected,
    /// Dispatch may have occurred but no authoritative receipt was validated.
    OutcomeUnknown,
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
    pub fn send_message(&self, thread_id: &str, response: &str) -> WriteResult {
        let args = [
            "send".to_owned(),
            thread_id.to_owned(),
            "--prompt".to_owned(),
            response.to_owned(),
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
    pub fn list_sessions(&self) -> Option<Vec<Value>> {
        let output = self.invoke(&["ls", "-g", "--json"], Duration::from_secs(20));
        if output.exit_code != Some(0) {
            return None;
        }
        serde_json::from_str::<Vec<Value>>(&output.stdout).ok()
    }

    /// Read recent assistant output without parsing content as JSON.
    #[must_use]
    pub fn read_log_text(&self, thread_id: &str, tail: usize) -> Option<String> {
        let tail = tail.min(200).to_string();
        let output = self.invoke(
            &["logs", thread_id, "--tail", &tail, "--filter", "text"],
            Duration::from_secs(20),
        );
        (output.exit_code == Some(0)).then(|| output.stdout.trim().to_owned())
    }

    fn invoke(&self, args: &[&str], timeout: Duration) -> ProcessOutput {
        let args = args.iter().map(ToString::to_string).collect::<Vec<_>>();
        let env = self.child_environment();
        self.executor.execute(&self.binary, &args, &env, timeout)
    }

    fn child_environment(&self) -> HashMap<String, String> {
        let mut env = HashMap::from([("PASEO_PASSWORD".to_owned(), self.password.clone())]);
        for name in ["PATH", "HOME", "WSL_INTEROP", "WSLENV"] {
            if let Ok(value) = std::env::var(name) {
                env.insert(name.to_owned(), value);
            }
        }
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
    if output.timed_out || output.exit_code.is_none() {
        return WriteResult::OutcomeUnknown;
    }
    if output.exit_code != Some(0) {
        return if has_structured_cli_error(&output.stdout)
            || has_structured_cli_error(&output.stderr)
        {
            WriteResult::Rejected
        } else {
            WriteResult::OutcomeUnknown
        };
    }
    let Ok(value) = serde_json::from_str::<Value>(&output.stdout) else {
        return WriteResult::OutcomeUnknown;
    };
    for key in ["messageId", "message_id", "id"] {
        if let Some(identifier) = value.get(key).and_then(Value::as_str)
            && paseo_safety_core::Identifier::new(identifier).is_ok()
        {
            return WriteResult::Delivered {
                receiver_message_id: identifier.to_owned(),
            };
        }
    }
    WriteResult::OutcomeUnknown
}

fn has_structured_cli_error(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|value| value.get("error")?.get("code")?.as_str().map(str::to_owned))
        .is_some()
}

use anyhow::Context;
use serde::Serialize;
use std::io::{Stdout, StdoutLock, Write};

#[derive(Debug)]
pub struct StdoutJson {
    stdout_lock: StdoutLock<'static>,
}

impl StdoutJson {
    pub fn new() -> Self {
        let stdout = std::io::stdout();
        let stdout_lock = stdout.lock();
        StdoutJson { stdout_lock }
    }

    pub fn write<T>(&mut self, message: &T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        serde_json::to_writer(&mut self.stdout_lock, message).context("cannot write to stdout")?;
        self.stdout_lock
            .write_all(b"\n")
            .context("cannot write to stdout")?;
        Ok(())
    }
}

impl Default for StdoutJson {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Stdout> for StdoutJson {
    fn from(stdout: Stdout) -> Self {
        Self { stdout_lock: stdout.lock() }
    }
}

impl From<StdoutJson> for StdoutLock<'static> {
    fn from(stdout_json: StdoutJson) -> Self {
        stdout_json.stdout_lock
    }
}

unsafe impl Send for StdoutJson {}
unsafe impl Sync for StdoutJson {}
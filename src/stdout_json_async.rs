use anyhow::Context;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub struct AsyncStdoutJson {
    async_stdout: tokio::io::Stdout,
}

impl AsyncStdoutJson {
    pub fn new() -> Self {
        let async_stdout = tokio::io::stdout();
        AsyncStdoutJson { async_stdout }
    }

    pub async fn write<T>(&mut self, message: &T) -> anyhow::Result<()>
    where
        T: Serialize,
    {
        let msg_json = serde_json::to_string(message)?;
        self.async_stdout
            .write_all(msg_json.as_bytes())
            .await
            .context("cannot write msg to stdout")?;
        self.async_stdout
            .write_all(b"\n")
            .await
            .context("cannot write newline to stdout")?;
        Ok(())
    }
}

impl Default for AsyncStdoutJson {
    fn default() -> Self {
        Self::new()
    }
}

impl From<tokio::io::Stdout> for AsyncStdoutJson {
    fn from(stdout: tokio::io::Stdout) -> Self {
        Self {
            async_stdout: stdout,
        }
    }
}

impl From<AsyncStdoutJson> for tokio::io::Stdout {
    fn from(stdout_json: AsyncStdoutJson) -> Self {
        stdout_json.async_stdout
    }
}

unsafe impl Send for AsyncStdoutJson {}
unsafe impl Sync for AsyncStdoutJson {}

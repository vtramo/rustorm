mod mloop;
mod mloop_async;

use crate::Message;
use crate::payloads::InitPayload;
use anyhow::Context;
pub use mloop::main_loop;
pub use mloop_async::main_loop_async;
use std::io::BufRead;

pub fn init() -> anyhow::Result<Message<InitPayload>> {
    let stdin = std::io::stdin().lock();
    let mut stdin_lines = stdin.lines();
    let init_msg = stdin_lines
        .next()
        .context("first message should be init")??;
    Ok(serde_json::from_str::<Message<InitPayload>>(&init_msg)
        .context("first message should be init")?)
}

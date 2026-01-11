use std::fmt::Debug;
use crate::node::Node;
use crate::payloads::InitPayload;
use crate::stdout_json::StdoutJson;
use crate::Message;
use anyhow::Context;
use serde::de::DeserializeOwned;
use std::io::BufRead;

pub fn main_loop<N, T>() -> anyhow::Result<()>
where
    N: Node<T>,
    T: DeserializeOwned + Debug
{
    let stdin = std::io::stdin().lock();
    let mut stdout_json = StdoutJson::new();
    let mut stdin_lines = stdin.lines();

    let init_msg = stdin_lines
        .next()
        .context("first message should be init")??;
    let init_msg = serde_json::from_str::<Message<InitPayload>>(&init_msg)
        .context("first message should be init")?;
    let mut node = N::init(init_msg, &mut stdout_json)?;

    for stdin_line in stdin_lines {
        let stdin_line = stdin_line.context("failed to read from stdin")?;
        let msg = serde_json::from_str::<Message<T>>(&stdin_line)
            .context("msg deserialization failed")?;
        node.step(msg, &mut stdout_json)
            .context("node step function failed")?;
    }

    Ok(())
}

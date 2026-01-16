mod mloop;
mod mloop_async;

use crate::payloads::{InitOkPayload, InitPayload};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};
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

pub fn receive_init_then_send_init_ok() -> anyhow::Result<String> {
    let Message {
        src,
        dst,
        body:
            Body {
                msg_id,
                in_reply_to,
                payload: InitPayload { node_id, node_ids },
            },
    } = init()?;

    let mut stdout_json = StdoutJson::new();
    let init_ok = Message {
        src: node_id.clone(),
        dst: src,
        body: Body {
            msg_id: None,
            in_reply_to: msg_id,
            payload: InitOkPayload::new(),
        },
    };
    stdout_json.write(&init_ok)?;

    Ok(node_id)
}

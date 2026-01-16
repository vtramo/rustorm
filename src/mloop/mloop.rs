use crate::Message;
use crate::node::Node;
use crate::payloads::{Event, InitPayload};
use crate::stdout_json::StdoutJson;
use anyhow::Context;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::io::BufRead;
use crate::mloop::init;

pub fn main_loop<N, P, IP>() -> anyhow::Result<()>
where
    N: Node<P, IP>,
    P: Debug + Send + 'static,
    IP: Debug + Send + 'static,
    Message<P>: DeserializeOwned,
{
    let init_msg = init()?;
    let (tx, rx) = std::sync::mpsc::channel::<Event<P, IP>>();
    let tx_stdin = tx.clone();
    std::thread::spawn::<_, anyhow::Result<()>>(move || {
        let tx = tx_stdin.clone();
        let stdin = std::io::stdin().lock();
        for stdin_line in stdin.lines() {
            let stdin_line = stdin_line.context("failed to read from stdin")?;
            let msg = serde_json::from_str::<Message<P>>(&stdin_line)
                .context("msg deserialization failed")?;
            if let Err(_) = tx.send(Event::Message(msg)) {
                return Ok(());
            };
        }
        Ok(())
    });

    let mut stdout_json = StdoutJson::new();
    let mut node = N::init(init_msg, &mut stdout_json, tx)?;
    for event in rx {
        node.step(event, &mut stdout_json)
            .context("node step function failed")?;
    }

    Ok(())
}

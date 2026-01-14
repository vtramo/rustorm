pub mod broadcast;
pub mod echo;
pub mod generate;
pub mod gocounter;
pub mod multibroadcast;
pub mod kafkalog;

use crate::payloads::{Event, InitOkPayload, InitPayload};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};
use std::fmt::Debug;

pub trait Node<P, IP = ()>
where
    P: Debug,
    IP: Debug,
{
    fn init(
        init_msg: Message<InitPayload>,
        output: &mut StdoutJson,
        tx_channel: std::sync::mpsc::Sender<Event<P, IP>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn step(&mut self, event: Event<P, IP>, output: &mut StdoutJson) -> anyhow::Result<()>;
}

fn common_init_node(
    init_msg: Message<InitPayload>,
    output: &mut StdoutJson,
) -> anyhow::Result<(String, Vec<String>)> {
    let node_id = init_msg.body.payload.node_id;

    let init_ok = Message {
        src: node_id.clone(),
        dst: init_msg.src.clone(),
        body: Body {
            msg_id: None,
            in_reply_to: init_msg.body.msg_id,
            payload: InitOkPayload::new(),
        },
    };

    output.write(&init_ok)?;
    Ok((node_id, init_msg.body.payload.node_ids))
}

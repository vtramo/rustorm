pub mod echo;
pub mod generate;

use crate::payloads::{InitOkPayload, InitPayload};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};

pub trait Node<T> {
    fn init(init_msg: Message<InitPayload>, output: &mut StdoutJson) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn step(&mut self, input: Message<T>, output: &mut StdoutJson) -> anyhow::Result<()>;
}

fn common_init_node(
    init_msg: Message<InitPayload>,
    output: &mut StdoutJson,
) -> anyhow::Result<String> {
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
    Ok(node_id)
}

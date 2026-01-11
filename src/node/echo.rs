use crate::node::{common_init_node, Node};
use crate::payloads::{EchoPayload, InitPayload};
use crate::stdout_json::StdoutJson;
use crate::Message;

pub struct EchoNode {
    pub id: String,
    pub msg_id: usize,
}

impl Node<EchoPayload> for EchoNode {
    fn init(init_msg: Message<InitPayload>, output: &mut StdoutJson) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (node_id, _) = common_init_node(init_msg, output)?;
        let echo_node = EchoNode {
            id: node_id,
            msg_id: 1,
        };
        Ok(echo_node)
    }

    fn step(
        &mut self,
        input_msg: Message<EchoPayload>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        let mut reply = input_msg.into_reply(Some(&mut self.msg_id));
        match reply.body.payload {
            EchoPayload::Echo { echo } => {
                reply.body.payload = EchoPayload::EchoOk { echo };
                output.write(&reply)?;
            }
            EchoPayload::EchoOk { .. } => {}
        };
        Ok(())
    }
}
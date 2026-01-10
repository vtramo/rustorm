use crate::node::{Node, common_init_node};
use crate::payloads::{EchoPayload, InitPayload};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};

pub struct EchoNode {
    pub id: String,
    pub msg_id: usize,
}

impl Node<EchoPayload> for EchoNode {
    fn init(init_msg: Message<InitPayload>, output: &mut StdoutJson) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node_id = common_init_node(init_msg, output)?;
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
        match input_msg.body.payload {
            EchoPayload::Echo { echo } => {
                let reply = Message {
                    src: self.id.clone(),
                    dst: input_msg.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input_msg.body.msg_id,
                        payload: EchoPayload::EchoOk { echo },
                    },
                };

                output.write(&reply)?;

                self.msg_id += 1;

                Ok(())
            }
            EchoPayload::EchoOk { .. } => Ok(()),
        }
    }
}

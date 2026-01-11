use crate::node::{Node, common_init_node};
use crate::payloads::{BroadcastPayload, InitPayload};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};

#[derive(Debug, Clone)]
pub struct BroadcastNode {
    pub id: String,
    pub msg_id: usize,
    pub node_ids: Vec<String>,
    pub broadcast_messages: Vec<usize>,
}

impl Node<BroadcastPayload> for BroadcastNode {
    fn init(init_msg: Message<InitPayload>, output: &mut StdoutJson) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (node_id, node_ids) = common_init_node(init_msg, output)?;
        Ok(BroadcastNode {
            id: node_id,
            msg_id: 0,
            node_ids,
            broadcast_messages: Vec::new(),
        })
    }

    fn step(
        &mut self,
        input: Message<BroadcastPayload>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            BroadcastPayload::Broadcast { message } => {
                self.broadcast_messages.push(message);

                self.broadcast_message(message, output)?;

                let broadcast_ok = Message {
                    src: self.id.clone(),
                    dst: input.src,
                    body: Body {
                        msg_id: None,
                        in_reply_to: input.body.msg_id,
                        payload: BroadcastPayload::BroadcastOk,
                    },
                };
                output.write(&broadcast_ok)?;
            }
            BroadcastPayload::Read => {
                let read_ok = Message {
                    src: self.id.clone(),
                    dst: input.src,
                    body: Body {
                        msg_id: None,
                        in_reply_to: input.body.msg_id,
                        payload: BroadcastPayload::ReadOk {
                            messages: self.broadcast_messages.clone(),
                        },
                    },
                };
                output.write(&read_ok)?;
            }
            BroadcastPayload::Topology { topology: _ } => {
                let topology_ok = Message {
                    src: self.id.clone(),
                    dst: input.src,
                    body: Body {
                        msg_id: None,
                        in_reply_to: input.body.msg_id,
                        payload: BroadcastPayload::TopologyOk,
                    },
                };
                output.write(&topology_ok)?;
            }
            BroadcastPayload::TopologyOk { .. }
            | BroadcastPayload::ReadOk { .. }
            | BroadcastPayload::BroadcastOk => {}
        };

        Ok(())
    }
}

impl BroadcastNode {
    fn broadcast_message(&self, msg: usize, output: &mut StdoutJson) -> anyhow::Result<()> {
        for node_id in &self.node_ids {}

        Ok(())
    }
}

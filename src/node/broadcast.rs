use crate::node::{common_init_node, Node};
use crate::payloads::{BroadcastPayload, InitPayload};
use crate::stdout_json::StdoutJson;
use crate::Message;

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
        let mut reply = input.into_reply(Some(&mut self.msg_id));
        match reply.body.payload {
            BroadcastPayload::Broadcast { message } => {
                self.broadcast_messages.push(message);
                self.broadcast_message(message, output)?;
                reply.body.payload = BroadcastPayload::BroadcastOk;
                output.write(&reply)?;
            }
            BroadcastPayload::Read => {
                reply.body.payload = BroadcastPayload::ReadOk {
                    messages: self.broadcast_messages.clone(),
                };
                output.write(&reply)?;
            }
            BroadcastPayload::Topology { topology: _ } => {
                reply.body.payload = BroadcastPayload::TopologyOk;
                output.write(&reply)?;
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

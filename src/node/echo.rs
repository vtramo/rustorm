use crate::Message;
use crate::node::{Node, common_init_node};
use crate::payloads::{EchoPayload, Event, InitPayload};
use crate::stdout_json::StdoutJson;

#[derive(Debug, Clone)]
pub struct EchoNode {
    pub id: String,
    pub msg_id: usize,
}

impl Node<EchoPayload> for EchoNode {
    fn init(
        init_msg: Message<InitPayload>,
        output: &mut StdoutJson,
        _tx_channel: std::sync::mpsc::Sender<Event<EchoPayload, ()>>,
    ) -> anyhow::Result<Self>
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
        event: Event<EchoPayload, ()>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        let Event::Message(input) = event else {
            panic!("only message events are allowed");
        };
        let mut reply = input.into_reply(Some(&mut self.msg_id));
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

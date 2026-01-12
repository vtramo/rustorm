use crate::Message;
use crate::node::{Node, common_init_node};
use crate::payloads::GeneratePayload::GenerateOk;
use crate::payloads::{Event, GeneratePayload, InitPayload};
use crate::stdout_json::StdoutJson;
use anyhow::Context;

#[derive(Debug, Clone)]
pub struct GenerateNode {
    pub id: String,
    pub msg_id: usize,
}

impl Node<GeneratePayload> for GenerateNode {
    fn init(
        init_msg: Message<InitPayload>,
        output: &mut StdoutJson,
        _tx_channel: std::sync::mpsc::Sender<Event<GeneratePayload, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (node_id, _) = common_init_node(init_msg, output)?;
        let generate_node = GenerateNode {
            id: node_id,
            msg_id: 1,
        };
        Ok(generate_node)
    }

    fn step(
        &mut self,
        event: Event<GeneratePayload, ()>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        let Event::Message(input) = event else {
            panic!("only message events are allowed");
        };
        let mut reply = input.into_reply(Some(&mut self.msg_id));
        match reply.body.payload {
            GeneratePayload::Generate { .. } => {
                reply.body.payload = GenerateOk {
                    guid: self.generate_guid_using_node_id(),
                };
                output.write(&reply)?;
            }
            GenerateOk { .. } => {}
        };
        Ok(())
    }
}

impl GenerateNode {
    #[allow(dead_code)]
    fn generate_ulid(&self) -> anyhow::Result<String> {
        Ok(ulid::Generator::new()
            .generate()
            .context("failed to generate ulid")?
            .to_string())
    }

    fn generate_guid_using_node_id(&self) -> String {
        format!("{}-{}", self.id, self.msg_id)
    }
}

use crate::node::{common_init_node, Node};
use crate::payloads::GeneratePayload::GenerateOk;
use crate::payloads::{GeneratePayload, InitPayload};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};
use anyhow::Context;

pub struct GenerateNode {
    pub id: String,
    pub msg_id: usize,
}

impl Node<GeneratePayload> for GenerateNode {
    fn init(init_msg: Message<InitPayload>, output: &mut StdoutJson) -> anyhow::Result<Self>
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
        input: Message<GeneratePayload>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            GeneratePayload::Generate { .. } => {
                let generate_ok = Message {
                    src: self.id.clone(),
                    dst: input.src,
                    body: Body {
                        msg_id: Some(self.msg_id),
                        in_reply_to: input.body.msg_id,
                        payload: GenerateOk {
                            guid: self.generate_guid_using_node_id(),
                        },
                    },
                };

                self.msg_id += 1;

                output.write(&generate_ok)?;
            }
            GenerateOk { .. } => {}
        };

        Ok(())
    }
}

impl GenerateNode {
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

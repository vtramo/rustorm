pub mod stdout_json;

use crate::stdout_json::StdoutJson;
use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub src: String,

    #[serde(rename = "dest")]
    pub dst: String,

    pub body: Body,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,

    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Generate,
    GenerateOk,
}

pub struct EchoNode {
    pub id: Option<String>,
    pub msg_id: usize,
}

impl EchoNode {
    pub fn step(&mut self, input_msg: Message, output: &mut StdoutJson) -> anyhow::Result<()> {
        match input_msg.body.payload {
            Payload::Init { node_id, .. } => {
                self.id = Some(node_id);
                let init_reply = Message {
                    src: self.id.clone().context("Missing node id")?,
                    dst: input_msg.src.clone(),
                    body: Body {
                        id: Some(self.msg_id),
                        in_reply_to: input_msg.body.id,
                        payload: Payload::InitOk,
                    },
                };

                output.write(&init_reply)?;

                Ok(())
            }

            Payload::Echo { echo } => {
                let reply = Message {
                    src: self.id.clone().context("Missing node id")?,
                    dst: input_msg.src,
                    body: Body {
                        id: Some(self.msg_id),
                        in_reply_to: input_msg.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };

                output.write(&reply)?;

                self.msg_id += 1;

                Ok(())
            }

            Payload::InitOk => bail!("received init_ok message"),
            Payload::Generate => bail!("received generate message"),
            Payload::GenerateOk => bail!("received generate_ok message"),
            Payload::EchoOk { .. } => Ok(()),
        }
    }
}

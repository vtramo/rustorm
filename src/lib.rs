pub mod mloop;
pub mod node;
pub mod payloads;
pub mod stdout_json;
pub mod stdout_json_async;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, Serialize)]
pub struct Message<T>
where
    T: Debug,
{
    pub src: String,

    #[serde(rename = "dest")]
    pub dst: String,

    pub body: Body<T>,
}

macro_rules! impl_message_deserialize {
    ($($payload:ty),* $(,)?) => {
        $(
            impl<'de> serde::Deserialize<'de> for Message<$payload> {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    #[derive(Deserialize)]
                    struct MessageHelper<T> {
                        src: String,
                        #[serde(rename = "dest")]
                        dst: String,
                        body: Body<T>,
                    }
                    let helper = MessageHelper::<$payload>::deserialize(deserializer)?;
                    Ok(Message {
                        src: helper.src,
                        dst: helper.dst,
                        body: helper.body,
                    })
                }
            }
        )*
    };
}

impl_message_deserialize!(
    payloads::EchoPayload,
    payloads::GeneratePayload,
    payloads::BroadcastPayload,
    payloads::InitPayload,
    payloads::KafkaLogPayload,
);

impl<T> Message<T>
where
    T: Debug,
{
    pub fn into_reply(self, id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                msg_id: id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
                }),
                in_reply_to: self.body.msg_id,
                payload: self.body.payload,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<T> {
    #[serde(rename = "msg_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: T,
}

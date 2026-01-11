pub mod mloop;
pub mod node;
pub mod payloads;
pub mod stdout_json;

use std::fmt::Debug;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<T> where T: Debug {
    pub src: String,

    #[serde(rename = "dest")]
    pub dst: String,

    pub body: Body<T>,
}

impl<T> Message<T> where T: Debug {
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
            }
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

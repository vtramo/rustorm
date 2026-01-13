use crate::Message;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::marker::{Send, Sync};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}
unsafe impl Send for EchoPayload {}
unsafe impl Sync for EchoPayload {}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InitPayload {
    pub node_id: String,
    pub node_ids: Vec<String>,
}
unsafe impl Send for InitPayload {}
unsafe impl Sync for InitPayload {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InitOkPayload {
    #[serde(rename = "type")]
    pub ty: String,
}
unsafe impl Send for InitOkPayload {}
unsafe impl Sync for InitOkPayload {}

impl InitOkPayload {
    const TYPE: &'static str = "init_ok";
    pub fn new() -> Self {
        InitOkPayload {
            ty: InitOkPayload::TYPE.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum GeneratePayload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}
unsafe impl Send for GeneratePayload {}
unsafe impl Sync for GeneratePayload {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum BroadcastPayload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<usize>,
    },
    GossipOk,
}
unsafe impl Send for BroadcastPayload {}
unsafe impl Sync for BroadcastPayload {}

#[derive(Debug, Clone)]
pub enum InjectedPayload {
    Gossip,
}
unsafe impl Send for InjectedPayload {}
unsafe impl Sync for InjectedPayload {}

#[derive(Debug, Clone)]
pub enum Event<P, IP>
where
    P: Debug,
    IP: Debug,
{
    Message(Message<P>),
    InjectedPayload(IP),
}

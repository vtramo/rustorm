use crate::{Body, Message};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use serde_core::Serializer;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InitPayload {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InitOkPayload {
    #[serde(rename = "type")]
    pub ty: String,
}

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

#[derive(Debug, Clone)]
pub enum InjectedPayload {
    Gossip,
}

#[derive(Debug, Clone)]
pub enum Event<P, IP>
where
    P: Debug,
    IP: Debug,
{
    Message(Message<P>),
    InjectedPayload(IP),
}

#[derive(Debug, Clone, Deserialize)]
pub enum GoCounterOrSeqKvPayload {
    GoCounter(GoCounterPayload),
    SeqKv(KvPayload),
}

impl Serialize for GoCounterOrSeqKvPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            GoCounterOrSeqKvPayload::GoCounter(go_counter_payload) => {
                go_counter_payload.serialize(serializer)
            }
            GoCounterOrSeqKvPayload::SeqKv(seq_kv_payload) => seq_kv_payload.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Message<GoCounterOrSeqKvPayload> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut json = serde_json::Value::deserialize(deserializer)?;

        let src = json
            .get("src")
            .and_then(|s| s.as_str())
            .ok_or_else(|| Error::missing_field("src"))?
            .to_string();

        let dest = json
            .get("dest")
            .and_then(|s| s.as_str())
            .ok_or_else(|| Error::missing_field("dest"))?
            .to_string();

        let body_val = json
            .get_mut("body")
            .ok_or_else(|| Error::missing_field("body"))?
            .take();

        if src == "seq-kv" {
            let body = serde_json::from_value::<Body<KvPayload>>(body_val)
                .map_err(|_| Error::custom("failed to deserialize body seq kv"))?;

            Ok(Message {
                src,
                dst: dest,
                body: Body {
                    msg_id: body.msg_id,
                    in_reply_to: body.in_reply_to,
                    payload: GoCounterOrSeqKvPayload::SeqKv(body.payload),
                },
            })
        } else {
            let body = serde_json::from_value::<Body<GoCounterPayload>>(body_val)
                .map_err(|_| Error::custom("failed to deserialize body seq kv"))?;

            Ok(Message {
                src,
                dst: dest,
                body: Body {
                    msg_id: body.msg_id,
                    in_reply_to: body.in_reply_to,
                    payload: GoCounterOrSeqKvPayload::GoCounter(body.payload),
                },
            })
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum GoCounterPayload {
    Read,
    ReadOk { value: usize },
    Add { delta: usize },
    AddOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum KvPayload {
    Read {
        key: String,
    },
    ReadOk {
        value: usize,
    },
    Write {
        key: String,
        value: usize,
    },
    WriteOk,
    Cas {
        key: String,
        from: usize,
        to: usize,
        create_if_not_exists: bool,
    },
    CasOk,
    Error {
        code: usize,
        text: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncCounter {
    Sync,
    CheckWrites,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum KafkaLogPayload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

#[derive(Debug, Clone)]
pub enum KafkaLogOrKvPayload {
    KafkaLog(KafkaLogPayload),
    Kv(KvPayload),
}


impl Serialize for KafkaLogOrKvPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            KafkaLogOrKvPayload::KafkaLog(kafka_log_payload) => {
                kafka_log_payload.serialize(serializer)
            }
            KafkaLogOrKvPayload::Kv(kv_payload) => kv_payload.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Message<KafkaLogOrKvPayload> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut json = serde_json::Value::deserialize(deserializer)?;

        let src = json
            .get("src")
            .and_then(|s| s.as_str())
            .ok_or_else(|| Error::missing_field("src"))?
            .to_string();

        let dest = json
            .get("dest")
            .and_then(|s| s.as_str())
            .ok_or_else(|| Error::missing_field("dest"))?
            .to_string();

        let body_val = json
            .get_mut("body")
            .ok_or_else(|| Error::missing_field("body"))?
            .take();

        if src == "seq-kv" || src == "lin-kv" {
            let body = serde_json::from_value::<Body<KvPayload>>(body_val)
                .map_err(|_| Error::custom("failed to deserialize body kv"))?;

            Ok(Message {
                src,
                dst: dest,
                body: Body {
                    msg_id: body.msg_id,
                    in_reply_to: body.in_reply_to,
                    payload: KafkaLogOrKvPayload::Kv(body.payload),
                },
            })
        } else {
            let body = serde_json::from_value::<Body<KafkaLogPayload>>(body_val)
                .map_err(|_| Error::custom("failed to deserialize body seq kv"))?;

            Ok(Message {
                src,
                dst: dest,
                body: Body {
                    msg_id: body.msg_id,
                    in_reply_to: body.in_reply_to,
                    payload: KafkaLogOrKvPayload::KafkaLog(body.payload),
                },
            })
        }
    }
}

pub struct KvErrorCode;
impl KvErrorCode {
    pub const CAS_ERROR: usize = 22;
    pub const KEY_NOT_FOUND: usize = 20;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum TxnPayload {
    Txn { txn: Vec<TxnOperation> },
    TxnOk { txn: Vec<TxnOperation> },
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum TxnOperation {
    Read { key: usize, value: Option<usize> },
    Write { key: usize, value: Option<usize> },
}

impl Serialize for TxnOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer
    {
        match self {
            TxnOperation::Read { key, value } => {
                ("r", key, value).serialize(serializer)
            }
            TxnOperation::Write { key, value } => {
                ("w", key, value).serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for TxnOperation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>
    {
        let json = serde_json::Value::deserialize(deserializer)?;
        let array = json.as_array().expect("operation is required to be an array");
        let operation_type = array[0].as_str().expect("operation type is required");
        match operation_type {
            "r" => {
                let key = array[1].as_u64().expect("key is required") as usize;
                let value = array[2].as_u64().map(|v| v as usize);
                Ok(TxnOperation::Read { key, value })
            }
            "w" => {
                let key = array[1].as_u64().expect("key is required") as usize;
                let value = array[2].as_u64().map(|v| v as usize);
                Ok(TxnOperation::Write { key, value })
            }
            _ => Err(Error::custom("invalid operation type")),
        }
    }
}
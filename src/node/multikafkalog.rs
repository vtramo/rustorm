use crate::payloads::{KafkaLogOrKvPayload, KafkaLogPayload, KvPayload};
use crate::{Body, Message};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct MultiKafkaLogNode {
    id: String,
    msg_generator: Arc<MsgGenerator>,
    stdin_channel_rx: tokio::sync::mpsc::UnboundedReceiver<Message<KafkaLogOrKvPayload>>,
    stdout_channel_tx: tokio::sync::mpsc::UnboundedSender<Message<KafkaLogOrKvPayload>>,
    log_channel_tx: tokio::sync::mpsc::UnboundedSender<LogMessage>,
    log_channel_rx: tokio::sync::mpsc::UnboundedReceiver<LogMessage>,
    log_by_key: HashMap<String, Arc<AsyncKafkaLog>>, // Remove Arc?
}

impl MultiKafkaLogNode {
    pub fn new(
        node_id: String,
        stdin_channel_rx: tokio::sync::mpsc::UnboundedReceiver<Message<KafkaLogOrKvPayload>>,
        stdout_channel_tx: tokio::sync::mpsc::UnboundedSender<Message<KafkaLogOrKvPayload>>,
    ) -> Self {
        let (log_channel_tx, log_channel_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            id: node_id,
            msg_generator: Arc::new(MsgGenerator::new()),
            stdin_channel_rx,
            stdout_channel_tx,
            log_channel_tx,
            log_channel_rx,
            log_by_key: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(stdin_msg) = self.stdin_channel_rx.recv() => {
                    match stdin_msg.body.payload {
                        KafkaLogOrKvPayload::KafkaLog(kafka_log_payload) => {
                            match kafka_log_payload {
                                KafkaLogPayload::Send { key, msg } => {
                                    let log = self.log_by_key
                                        .entry(key.to_string())
                                        .or_insert(Arc::new(AsyncKafkaLog::new(
                                            key.to_string(),
                                            self.id.clone(),
                                            self.log_channel_tx.clone(),
                                            self.stdout_channel_tx.clone(),
                                            Arc::clone(&self.msg_generator),
                                        )));

                                    let send_id = NodeMsgId::new(
                                        stdin_msg.src,
                                        stdin_msg.body.msg_id.expect("msg_id is required (send)"),
                                    );
                                    let log = Arc::clone(&log);
                                    log.send(send_id, msg);
                                },
                                _ => {}
                            }
                        },
                        KafkaLogOrKvPayload::Kv(kv_payload) => {
                            match kv_payload {
                                KvPayload::CasOk => {
                                    let in_reply_to = stdin_msg
                                        .body
                                        .in_reply_to
                                        .expect("in_reply_to is required (cas_ok)");

                                    let log_key = self.msg_generator
                                        .consume_log_key(in_reply_to)
                                        .expect(&format!("log_key not found for msg_id {}", in_reply_to));

                                    let log = self.log_by_key
                                        .get(&log_key)
                                        .expect(&format!("log not found for key {}", log_key));

                                    let (send_id, offset) = log.cas_ok(in_reply_to);
                                    self.stdout_channel_tx.send(Message {
                                        src: self.id.clone(),
                                        dst: send_id.node_id,
                                        body: Body {
                                            msg_id: Some(self.msg_generator.generate_msg_id()),
                                            in_reply_to: Some(send_id.msg_id),
                                            payload: KafkaLogOrKvPayload::KafkaLog(
                                                KafkaLogPayload::SendOk { offset }
                                            )
                                        },
                                    }).expect("failed to write to seq-kv");
                                },
                                KvPayload::Error { code, text: _text } => {
                                    let in_reply_to = stdin_msg
                                        .body
                                        .in_reply_to
                                        .expect("in_reply_to is required (cas error)");

                                    let log_key = self.msg_generator
                                        .consume_log_key(in_reply_to)
                                        .expect(&format!("log_key not found for msg_id {}", in_reply_to));

                                    let log = self.log_by_key
                                        .get(&log_key)
                                        .expect(&format!("log not found for key {}", log_key));

                                    log.cas_error(in_reply_to);
                                },
                                KvPayload::ReadOk { value: updated_offset } => {
                                    let in_reply_to = stdin_msg
                                        .body
                                        .in_reply_to
                                        .expect("in_reply_to is required (read ok)");

                                    let log_key = self.msg_generator
                                        .consume_log_key(in_reply_to)
                                        .expect(&format!("log_key not found for msg_id {}", in_reply_to));

                                    let log = self.log_by_key
                                        .get(&log_key)
                                        .expect(&format!("log not found for key {}", log_key));

                                    log.update_offset(in_reply_to, updated_offset);
                                }
                                _ => {}
                            }
                        },
                    }
                },
                Some(log_msg) = self.log_channel_rx.recv() => {

                },
            }
        }
    }
}

enum LogMessage {
    SendOk { send_id: usize },
}

#[derive(Debug)]
struct MsgGenerator {
    free_msg_id: AtomicUsize,
    log_key_by_msg_id: DashMap<usize, String>,
}

impl MsgGenerator {
    fn new() -> Self {
        Self {
            free_msg_id: AtomicUsize::new(0),
            log_key_by_msg_id: DashMap::new(),
        }
    }

    fn generate_msg_id(&self) -> usize {
        self.free_msg_id.fetch_add(1, Ordering::Relaxed)
    }

    fn consume_log_key(&self, msg_id: usize) -> Option<String> {
        self.log_key_by_msg_id
            .remove(&msg_id)
            .map(|log_key| log_key.1)
    }

    fn generate_log_msg_id(&self, log_key: String) -> usize {
        let msg_id = self.generate_msg_id();
        self.log_key_by_msg_id.insert(msg_id, log_key);
        msg_id
    }
}

unsafe impl Send for MsgGenerator {}
unsafe impl Sync for MsgGenerator {}

#[derive(Debug)]
struct AsyncKafkaLog {
    key: String,
    node_id: String,
    log_channel_tx: tokio::sync::mpsc::UnboundedSender<LogMessage>,
    stdout_channel_tx: tokio::sync::mpsc::UnboundedSender<Message<KafkaLogOrKvPayload>>,
    msg_generator: Arc<MsgGenerator>,
    semantics_by_msg_id: DashMap<usize, LogMsgSemantics>,
    local_offset: AtomicUsize,
}

impl AsyncKafkaLog {
    const LIN_KV: &'static str = "lin-kv";
    const SEQ_KV: &'static str = "seq-kv";

    fn new(
        key: String,
        node_id: String,
        log_channel_tx: tokio::sync::mpsc::UnboundedSender<LogMessage>,
        stdout_channel_tx: tokio::sync::mpsc::UnboundedSender<Message<KafkaLogOrKvPayload>>,
        msg_generator: Arc<MsgGenerator>,
    ) -> Self {
        Self {
            key,
            node_id,
            log_channel_tx,
            stdout_channel_tx,
            msg_generator,
            semantics_by_msg_id: DashMap::new(),
            local_offset: AtomicUsize::new(0),
        }
    }

    fn send(&self, send_id: NodeMsgId, msg: usize) {
        let cas_msg_id = self.msg_generator.generate_log_msg_id(self.key.clone());
        let local_offset = self.local_offset.load(Ordering::Relaxed);
        self.semantics_by_msg_id.insert(
            cas_msg_id,
            LogMsgSemantics::Cas {
                send_id,
                msg,
                offset: local_offset + 1,
            },
        );
        if let Err(_) = self.stdout_channel_tx.send(Message {
            src: self.node_id.clone(),
            dst: Self::LIN_KV.to_string(),
            body: Body {
                msg_id: Some(cas_msg_id),
                in_reply_to: None,
                payload: KafkaLogOrKvPayload::Kv(KvPayload::Cas {
                    key: self.log_offset_key(),
                    from: local_offset,
                    to: local_offset + 1,
                    create_if_not_exists: true,
                }),
            },
        }) {}
    }

    fn log_offset_key(&self) -> String {
        format!("offset-{}", self.key)
    }

    fn cas_ok(&self, cas_msg_id: usize) -> (NodeMsgId, usize) {
        let LogMsgSemantics::Cas {
            send_id,
            msg,
            offset,
        } = self
            .semantics_by_msg_id
            .remove(&cas_msg_id)
            .expect("cas_ok called for unknown msg_id")
            .1
        else {
            panic!("cas_ok called for unknown msg_id")
        };

        self.local_offset.store(offset, Ordering::Relaxed);
        let write_msg_id = self.msg_generator.generate_log_msg_id(self.key.clone());
        self.stdout_channel_tx
            .send(Message {
                src: self.node_id.clone(),
                dst: Self::SEQ_KV.to_string(),
                body: Body {
                    msg_id: Some(write_msg_id),
                    in_reply_to: None,
                    payload: KafkaLogOrKvPayload::Kv(KvPayload::Write {
                        key: format!("{}-{}", self.key, offset),
                        value: msg,
                    }),
                },
            })
            .expect("failed to write to seq-kv");
        (send_id, offset)
    }

    fn cas_error(&self, cas_msg_id: usize) {
        let LogMsgSemantics::Cas {
            send_id,
            msg,
            offset: _offset,
        } = self
            .semantics_by_msg_id
            .remove(&cas_msg_id)
            .expect(&format!(
                "cas_error called for unknown msg_id {}",
                cas_msg_id
            ))
            .1
        else {
            panic!("cas_error called for unknown msg_id {}", cas_msg_id)
        };

        let read_msg_id = self.msg_generator.generate_log_msg_id(self.key.clone());
        self.stdout_channel_tx
            .send(Message {
                src: self.node_id.clone(),
                dst: Self::LIN_KV.to_string(),
                body: Body {
                    msg_id: Some(read_msg_id),
                    in_reply_to: None,
                    payload: KafkaLogOrKvPayload::Kv(KvPayload::Read {
                        key: self.log_offset_key(),
                    }),
                },
            })
            .expect("failed to read from lin-kv");

        self.semantics_by_msg_id.insert(
            read_msg_id,
            LogMsgSemantics::ReadUpdatedOffset { send_id, msg },
        );
    }

    fn update_offset(&self, read_msg_id: usize, new_offset: usize) {
        let LogMsgSemantics::ReadUpdatedOffset { send_id, msg } = self
            .semantics_by_msg_id
            .remove(&read_msg_id)
            .expect(&format!(
                "update_offset called for unknown msg_id {}",
                read_msg_id
            ))
            .1
        else {
            panic!("update_offset called for unknown msg_id {}", read_msg_id)
        };

        self.local_offset.store(new_offset, Ordering::Relaxed);
        self.send(send_id, msg);
    }
}

#[derive(Debug)]
enum LogMsgSemantics {
    Cas {
        send_id: NodeMsgId,
        msg: usize,
        offset: usize,
    },
    ReadUpdatedOffset {
        send_id: NodeMsgId,
        msg: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeMsgId {
    node_id: String,
    msg_id: usize,
}

impl NodeMsgId {
    fn new(node_id: String, msg_id: usize) -> Self {
        Self { node_id, msg_id }
    }
}

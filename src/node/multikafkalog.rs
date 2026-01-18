use crate::payloads::{KafkaLogOrKvPayload, KafkaLogPayload, KvErrorCode, KvPayload};
use crate::{Body, Message};
use dashmap::DashMap;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct MultiKafkaLogNode {
    id: String,
    max_poll: usize,
    msg_generator: Arc<MsgGenerator>,
    stdin_channel_rx: tokio::sync::mpsc::UnboundedReceiver<Message<KafkaLogOrKvPayload>>,
    stdout_channel_tx: tokio::sync::mpsc::UnboundedSender<Message<KafkaLogOrKvPayload>>,
    log_channel_tx: tokio::sync::mpsc::UnboundedSender<LogMessage>,
    log_channel_rx: tokio::sync::mpsc::UnboundedReceiver<LogMessage>,
    log_by_key: HashMap<String, AsyncKafkaLog>,
    completed_polls: HashMap<NodeMsgId, Progress<(String, Vec<(usize, usize)>)>>,
    completed_commits: HashMap<NodeMsgId, Progress<String>>,
}

impl MultiKafkaLogNode {
    pub fn new(
        node_id: String,
        max_poll: usize,
        stdin_channel_rx: tokio::sync::mpsc::UnboundedReceiver<Message<KafkaLogOrKvPayload>>,
        stdout_channel_tx: tokio::sync::mpsc::UnboundedSender<Message<KafkaLogOrKvPayload>>,
    ) -> Self {
        let (log_channel_tx, log_channel_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            id: node_id,
            max_poll,
            msg_generator: Arc::new(MsgGenerator::new()),
            stdin_channel_rx,
            stdout_channel_tx,
            log_channel_tx,
            log_channel_rx,
            log_by_key: HashMap::new(),
            completed_polls: HashMap::new(),
            completed_commits: HashMap::new(),
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
                                        .or_insert(AsyncKafkaLog::new(
                                            key.to_string(),
                                            self.id.clone(),
                                            self.max_poll,
                                            self.log_channel_tx.clone(),
                                            self.stdout_channel_tx.clone(),
                                            Arc::clone(&self.msg_generator),
                                        ));

                                    let send_id = NodeMsgId::new(
                                        stdin_msg.src,
                                        stdin_msg.body.msg_id.expect("msg_id is required (send)"),
                                    );
                                    log.send(send_id, msg);
                                },
                                KafkaLogPayload::Poll { offsets } => {
                                    let dst_node_id = stdin_msg.src;
                                    let poll_msg_id = stdin_msg.body.msg_id.expect("msg_id is required (poll)");
                                    let poll_id = NodeMsgId::new(dst_node_id, poll_msg_id);

                                    let offsets = offsets
                                        .into_iter()
                                        .filter(|(key, _)| self.log_by_key.contains_key(key))
                                        .collect::<HashMap<_, _>>();

                                    if offsets.len() == 0 {
                                        let poll_ok = Message {
                                            src: self.id.clone(),
                                            dst: poll_id.node_id.clone(),
                                            body: Body {
                                                msg_id: Some(self.msg_generator.generate_msg_id()),
                                                in_reply_to: Some(poll_id.msg_id),
                                                payload: KafkaLogOrKvPayload::KafkaLog(
                                                    KafkaLogPayload::PollOk {
                                                        msgs: HashMap::new()
                                                    }
                                                )
                                            },
                                        };

                                        self.stdout_channel_tx.send(poll_ok)
                                            .expect("failed to write to seq-kv");
                                    } else {
                                        self.completed_polls.insert(
                                            poll_id.clone(),
                                            Progress::new(offsets.len())
                                        );

                                        for (key, offset) in offsets {
                                            let log = self.log_by_key.get_mut(&key).unwrap();
                                            log.poll(poll_id.clone(), offset);
                                        }
                                    }
                                },
                                KafkaLogPayload::CommitOffsets { offsets } => {
                                    eprintln!("COMMIT OFFSETS");
                                    let offsets = offsets
                                        .into_iter()
                                        .filter(|(key, _)| self.log_by_key.contains_key(key))
                                        .collect::<HashMap<_, _>>();

                                    let src = stdin_msg.src;
                                    let commit_msg_id = stdin_msg
                                        .body
                                        .msg_id
                                        .expect("msg_id is required (commit_offsets)");

                                    if offsets.len() == 0 {
                                        let commit_offset_ok = Message {
                                            src: self.id.clone(),
                                            dst: src,
                                            body: Body {
                                                msg_id: None,
                                                in_reply_to: None,
                                                payload: KafkaLogOrKvPayload::KafkaLog(
                                                    KafkaLogPayload::CommitOffsetsOk
                                                ),
                                            },
                                        };

                                        self.stdout_channel_tx
                                            .send(commit_offset_ok)
                                            .expect("failed to send commit_offsets_ok");
                                    } else {
                                        let commit_id = NodeMsgId::new(src, commit_msg_id);
                                        self.completed_commits.insert(
                                            commit_id.clone(),
                                            Progress::new(offsets.len())
                                        );
                                        for (log_key, offset) in offsets {
                                            let log = self.log_by_key.get_mut(&log_key).unwrap();
                                            match log.commit_offset(commit_id.clone(), offset) {
                                                OperationStatus::Completed => {
                                                    let completed_commits = self.completed_commits
                                                        .get_mut(&commit_id)
                                                        .unwrap();

                                                    completed_commits.push(log_key);
                                                    if completed_commits.is_completed() {

                                                    }
                                                }
                                                OperationStatus::InProgress => {
                                                    let commit_offset_ok = Message {
                                                        src: self.id.clone(),
                                                        dst: commit_id.node_id.clone(),
                                                        body: Body {
                                                            msg_id: None,
                                                            in_reply_to: Some(commit_id.msg_id),
                                                            payload: KafkaLogOrKvPayload::KafkaLog(
                                                                KafkaLogPayload::CommitOffsetsOk
                                                            ),
                                                        },
                                                    };

                                                    self.stdout_channel_tx
                                                        .send(commit_offset_ok)
                                                        .expect("failed to send commit_offsets_ok");
                                                }
                                                OperationStatus::PollCompleted { .. } => {
                                                    panic!("received poll completed for commit_offsets");
                                                }
                                            }
                                        }
                                    }
                                }
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

                                    match log.cas_ok(&NodeMsgId::new(self.id.clone(), in_reply_to)) {
                                        CasSemantics::SendCas { send_id, offset } => {
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
                                        CasSemantics::CommitCas { commit_id, offset: _offset } => {
                                            let commit_progress = self
                                                .completed_commits
                                                .get_mut(&commit_id)
                                                .expect("commit progress not found");

                                            commit_progress.push(log_key);
                                            if commit_progress.is_completed() {
                                                self.completed_commits.remove(&commit_id);
                                                self.stdout_channel_tx.send(Message {
                                                    src: self.id.clone(),
                                                    dst: commit_id.node_id,
                                                    body: Body {
                                                        msg_id: Some(self.msg_generator.generate_msg_id()),
                                                        in_reply_to: Some(commit_id.msg_id),
                                                        payload: KafkaLogOrKvPayload::KafkaLog(
                                                            KafkaLogPayload::CommitOffsetsOk
                                                        )
                                                    },
                                                }).expect("failed to write to seq-kv");
                                            }
                                        }
                                    }
                                },
                                KvPayload::Error { code, text: _text }
                                if code == KvErrorCode::CAS_ERROR => {
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

                                    log.cas_error(&NodeMsgId::new(self.id.clone(), in_reply_to));
                                },
                                KvPayload::Error { code, text: _text }
                                if code == KvErrorCode::KEY_NOT_FOUND => {
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

                                    match log.poll_read_missing_key(in_reply_to) {
                                        OperationStatus::PollCompleted { poll_id, msgs } => {
                                            let poll_progress = self.completed_polls
                                                .get_mut(&poll_id)
                                                .expect("poll progress not found");

                                            poll_progress.push((log_key, msgs));
                                            if poll_progress.is_completed() {
                                                let poll_ok = Message {
                                                    src: self.id.clone(),
                                                    dst: poll_id.node_id.clone(),
                                                    body: Body {
                                                        msg_id: Some(self.msg_generator.generate_msg_id()),
                                                        in_reply_to: Some(poll_id.msg_id),
                                                        payload: KafkaLogOrKvPayload::KafkaLog(
                                                            KafkaLogPayload::PollOk {
                                                                msgs: self
                                                                    .poll_completed(&poll_id)
                                                                    .expect("poll_id not found")
                                                            }
                                                        )
                                                    },
                                                };

                                                self.stdout_channel_tx.send(poll_ok)
                                                    .expect("failed to write to seq-kv");
                                            }
                                        }
                                        OperationStatus::Completed => {}
                                        OperationStatus::InProgress => {}
                                    }
                                },
                                KvPayload::ReadOk { value } => {
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

                                    match log.read_ok(in_reply_to, value) {
                                        OperationStatus::PollCompleted { poll_id, msgs } => {
                                            let poll_progress = self.completed_polls
                                                .get_mut(&poll_id)
                                                .expect("poll progress not found");
                                            poll_progress.push((log_key, msgs));

                                            if poll_progress.is_completed() {
                                                let poll_ok = Message {
                                                    src: self.id.clone(),
                                                    dst: poll_id.node_id.clone(),
                                                    body: Body {
                                                        msg_id: Some(self.msg_generator.generate_msg_id()),
                                                        in_reply_to: Some(poll_id.msg_id),
                                                        payload: KafkaLogOrKvPayload::KafkaLog(
                                                            KafkaLogPayload::PollOk {
                                                                msgs: self
                                                                    .poll_completed(&poll_id)
                                                                    .expect("poll_competed: poll_id not found")
                                                            }
                                                        )
                                                    },
                                                };

                                                self.stdout_channel_tx.send(poll_ok)
                                                    .expect("failed to write to seq-kv");
                                            }
                                        }
                                        OperationStatus::Completed => {}
                                        OperationStatus::InProgress => {}
                                    }
                                }
                                _ => {
                                    eprintln!("Unhandled KvPayload: {:?}", kv_payload);
                                }
                            }
                        },
                    }
                },
                Some(log_msg) = self.log_channel_rx.recv() => {

                },
            }
        }
    }

    fn poll_completed(
        &mut self,
        poll_id: &NodeMsgId,
    ) -> Option<HashMap<String, Vec<(usize, usize)>>> {
        self.completed_polls
            .remove(&poll_id)
            .map(|poll_progress| poll_progress.into())
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
    max_poll: usize,
    log_channel_tx: tokio::sync::mpsc::UnboundedSender<LogMessage>,
    stdout_channel_tx: tokio::sync::mpsc::UnboundedSender<Message<KafkaLogOrKvPayload>>,
    msg_generator: Arc<MsgGenerator>,
    semantics_by_msg_id: DashMap<NodeMsgId, LogMsgSemantics>,
    local_committed_offset: AtomicUsize,
    local_offset: AtomicUsize,
    completed_polls: DashMap<NodeMsgId, Vec<Option<(usize, usize)>>>,
}

impl AsyncKafkaLog {
    const LIN_KV: &'static str = "lin-kv";
    const SEQ_KV: &'static str = "seq-kv";

    fn new(
        key: String,
        node_id: String,
        max_poll: usize,
        log_channel_tx: tokio::sync::mpsc::UnboundedSender<LogMessage>,
        stdout_channel_tx: tokio::sync::mpsc::UnboundedSender<Message<KafkaLogOrKvPayload>>,
        msg_generator: Arc<MsgGenerator>,
    ) -> Self {
        if max_poll == 0 {
            panic!("max_poll must be greater than 0");
        }

        Self {
            key,
            max_poll,
            node_id,
            log_channel_tx,
            stdout_channel_tx,
            msg_generator,
            semantics_by_msg_id: DashMap::new(),
            local_committed_offset: AtomicUsize::new(0),
            local_offset: AtomicUsize::new(0),
            completed_polls: DashMap::new(),
        }
    }

    fn send(&self, send_id: NodeMsgId, msg: usize) {
        let cas_msg_id = self.msg_generator.generate_log_msg_id(self.key.clone());
        let local_offset = self.local_offset.load(Ordering::Relaxed);
        self.semantics_by_msg_id.insert(
            NodeMsgId::new(self.node_id.clone(), cas_msg_id),
            LogMsgSemantics::CasSend {
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

    fn cas_ok(&self, cas_id: &NodeMsgId) -> CasSemantics {
        match self
            .semantics_by_msg_id
            .remove(cas_id)
            .expect("cas_ok called for unknown msg_id")
            .1
        {
            LogMsgSemantics::CasSend {
                send_id,
                msg,
                offset,
            } => {
                let (send_id, offset) = self.cas_ok_send(send_id, msg, offset);
                CasSemantics::SendCas { send_id, offset }
            }
            LogMsgSemantics::CasCommitOffset { commit_id, offset } => {
                let (commit_id, offset) = self.cas_ok_commit_offset(commit_id, offset);
                CasSemantics::CommitCas { commit_id, offset }
            }
            LogMsgSemantics::ReadUpdatedOffset { .. } => {
                panic!("cas_ok called for ReadUpdateOffset")
            }
            LogMsgSemantics::ReadPollMessage { .. } => {
                panic!("cas_ok called for ReadPollMessage")
            }
        }
    }

    fn cas_ok_send(&self, send_id: NodeMsgId, msg: usize, offset: usize) -> (NodeMsgId, usize) {
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
                        key: self.msg_offset_key(offset),
                        value: msg,
                    }),
                },
            })
            .expect("failed to write to seq-kv");
        (send_id, offset)
    }

    fn cas_ok_commit_offset(&self, send_id: NodeMsgId, offset: usize) -> (NodeMsgId, usize) {
        self.local_committed_offset.store(offset, Ordering::Relaxed);
        (send_id, offset)
    }

    fn msg_offset_key(&self, offset: usize) -> String {
        format!("{}-{}", self.key, offset)
    }

    fn cas_error(&self, cas_id: &NodeMsgId) {
        let LogMsgSemantics::CasSend {
            send_id,
            msg,
            offset: _offset,
        } = self
            .semantics_by_msg_id
            .remove(cas_id)
            .expect(&format!("cas_error called for unknown msg_id {:?}", cas_id))
            .1
        else {
            panic!("cas_error called for unknown msg_id {:?}", cas_id)
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
            NodeMsgId::new(self.node_id.clone(), read_msg_id),
            LogMsgSemantics::ReadUpdatedOffset { send_id, msg },
        );
    }

    fn read_ok(&self, read_msg_id: usize, value: usize) -> OperationStatus {
        let read_semantics = match self
            .semantics_by_msg_id
            .get(&NodeMsgId::new(self.node_id.clone(), read_msg_id))
        {
            None => ReadSemantics::None,
            Some(entry) => match *entry.value() {
                LogMsgSemantics::ReadUpdatedOffset { .. } => ReadSemantics::ReadUpdatedOffset,
                LogMsgSemantics::ReadPollMessage { .. } => ReadSemantics::ReadPollMessage,
                LogMsgSemantics::CasSend { .. } => {
                    panic!("read_ok called for cas send msg_id {}", read_msg_id);
                }
                LogMsgSemantics::CasCommitOffset { .. } => {
                    panic!(
                        "read_ok called for cas commit offset msg_id {}",
                        read_msg_id
                    );
                }
            },
        };

        match read_semantics {
            ReadSemantics::None => OperationStatus::Completed,
            ReadSemantics::ReadUpdatedOffset => self.update_offset(read_msg_id, value),
            ReadSemantics::ReadPollMessage => self.poll_read_ok(read_msg_id, value),
        }
    }

    fn update_offset(&self, read_msg_id: usize, new_offset: usize) -> OperationStatus {
        let LogMsgSemantics::ReadUpdatedOffset { send_id, msg } = self
            .semantics_by_msg_id
            .remove(&NodeMsgId::new(self.node_id.clone(), read_msg_id))
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
        OperationStatus::InProgress
    }

    fn poll(&self, poll_id: NodeMsgId, offset: usize) {
        self.completed_polls
            .insert(poll_id.clone(), Vec::with_capacity(self.max_poll));
        for i in 0..self.max_poll {
            let read_msg_id = self.msg_generator.generate_log_msg_id(self.key.clone());
            let msg_offset = offset + i;
            self.semantics_by_msg_id.insert(
                NodeMsgId::new(self.node_id.clone(), read_msg_id),
                LogMsgSemantics::ReadPollMessage {
                    poll_id: poll_id.clone(),
                    offset: msg_offset,
                },
            );
            self.stdout_channel_tx
                .send(Message {
                    src: self.node_id.clone(),
                    dst: Self::SEQ_KV.to_string(),
                    body: Body {
                        msg_id: Some(read_msg_id),
                        in_reply_to: None,
                        payload: KafkaLogOrKvPayload::Kv(KvPayload::Read {
                            key: self.msg_offset_key(msg_offset),
                        }),
                    },
                })
                .expect("failed to read from lin-kv");
        }
    }

    fn poll_read_missing_key(&self, read_msg_id: usize) -> OperationStatus {
        let LogMsgSemantics::ReadPollMessage { poll_id, offset } = self
            .semantics_by_msg_id
            .remove(&NodeMsgId::new(self.node_id.clone(), read_msg_id))
            .expect(&format!(
                "poll_read_ok called for unknown msg_id {}",
                read_msg_id
            ))
            .1
        else {
            panic!("poll_read_ok called for unknown msg_id {}", read_msg_id)
        };

        let is_completed = {
            let mut completed_polls = self
                .completed_polls
                .get_mut(&poll_id)
                .expect("poll_id not found");

            completed_polls.push(None);
            completed_polls.len() == self.max_poll
        };

        if is_completed {
            let completed_pool = self
                .completed_polls
                .remove(&poll_id)
                .expect("poll_id not found");
            OperationStatus::PollCompleted {
                poll_id,
                msgs: completed_pool.1.into_iter().flatten().collect(),
            }
        } else {
            OperationStatus::InProgress
        }
    }

    fn poll_read_ok(&self, read_msg_id: usize, value: usize) -> OperationStatus {
        let LogMsgSemantics::ReadPollMessage { poll_id, offset } = self
            .semantics_by_msg_id
            .remove(&NodeMsgId::new(self.node_id.clone(), read_msg_id))
            .expect(&format!(
                "poll_read_ok called for unknown msg_id {}",
                read_msg_id
            ))
            .1
        else {
            panic!("poll_read_ok called for unknown msg_id {}", read_msg_id)
        };

        let is_completed = {
            let mut completed_polls = self
                .completed_polls
                .get_mut(&poll_id)
                .expect("poll_id not found");

            completed_polls.push(Some((offset, value)));
            completed_polls.len() == self.max_poll
        };

        if is_completed {
            let completed_pool = self
                .completed_polls
                .remove(&poll_id)
                .expect("poll_id not found");
            OperationStatus::PollCompleted {
                poll_id,
                msgs: completed_pool.1.into_iter().flatten().collect(),
            }
        } else {
            OperationStatus::InProgress
        }
    }

    fn commit_offset(&self, commit_id: NodeMsgId, offset: usize) -> OperationStatus {
        if self.local_committed_offset.load(Ordering::Relaxed) >= offset {
            return OperationStatus::Completed;
        }

        let cas_msg_id = self.msg_generator.generate_log_msg_id(self.key.clone());
        self.semantics_by_msg_id.insert(
            NodeMsgId::new(self.node_id.clone(), cas_msg_id),
            LogMsgSemantics::CasCommitOffset { commit_id, offset },
        );

        self.stdout_channel_tx
            .send(Message {
                src: self.node_id.clone(),
                dst: Self::LIN_KV.to_string(),
                body: Body {
                    msg_id: Some(cas_msg_id),
                    in_reply_to: None,
                    payload: KafkaLogOrKvPayload::Kv(KvPayload::Cas {
                        key: self.committed_offset_key(),
                        from: self.local_committed_offset.load(Ordering::Relaxed),
                        to: offset,
                        create_if_not_exists: true,
                    }),
                },
            })
            .expect("failed to write to lin-kv");

        OperationStatus::InProgress
    }

    fn committed_offset_key(&self) -> String {
        format!("committed-offset-{}", self.key)
    }
}

#[derive(Debug, Clone)]
enum OperationStatus {
    PollCompleted {
        poll_id: NodeMsgId,
        msgs: Vec<(usize, usize)>,
    },
    Completed,
    InProgress,
}

#[derive(Debug)]
enum LogMsgSemantics {
    CasSend {
        send_id: NodeMsgId,
        msg: usize,
        offset: usize,
    },
    CasCommitOffset {
        commit_id: NodeMsgId,
        offset: usize,
    },
    ReadUpdatedOffset {
        send_id: NodeMsgId,
        msg: usize,
    },
    ReadPollMessage {
        poll_id: NodeMsgId,
        offset: usize,
    },
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
enum ReadSemantics {
    ReadUpdatedOffset,
    ReadPollMessage,
    None,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
enum CasSemantics {
    SendCas { send_id: NodeMsgId, offset: usize },
    CommitCas { commit_id: NodeMsgId, offset: usize },
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Progress<T> {
    tot_log_keys: usize,
    progress: Vec<T>,
}

impl<T> Progress<T> {
    fn new(tot_log_keys: usize) -> Self {
        Self {
            tot_log_keys,
            progress: Vec::with_capacity(tot_log_keys),
        }
    }

    fn is_completed(&self) -> bool {
        self.progress.len() == self.tot_log_keys
    }
}

impl Deref for Progress<(String, Vec<(usize, usize)>)> {
    type Target = Vec<(String, Vec<(usize, usize)>)>;

    fn deref(&self) -> &Self::Target {
        &self.progress
    }
}

impl DerefMut for Progress<(String, Vec<(usize, usize)>)> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.progress
    }
}

impl From<HashMap<String, Vec<(usize, usize)>>> for Progress<(String, Vec<(usize, usize)>)> {
    fn from(value: HashMap<String, Vec<(usize, usize)>>) -> Self {
        Self {
            tot_log_keys: value.len(),
            progress: value.into_iter().collect(),
        }
    }
}

impl From<Progress<(String, Vec<(usize, usize)>)>> for HashMap<String, Vec<(usize, usize)>> {
    fn from(value: Progress<(String, Vec<(usize, usize)>)>) -> Self {
        value.progress.into_iter().collect()
    }
}

impl Deref for Progress<String> {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.progress
    }
}

impl DerefMut for Progress<String> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.progress
    }
}

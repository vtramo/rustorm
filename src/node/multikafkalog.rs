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
    input_channel_rx: tokio::sync::mpsc::UnboundedReceiver<Message<KafkaLogOrKvPayload>>,
    stdout_channel_tx: tokio::sync::mpsc::UnboundedSender<Message<KafkaLogOrKvPayload>>,
    log_channel_tx: tokio::sync::mpsc::UnboundedSender<LogMessage>,
    log_channel_rx: tokio::sync::mpsc::UnboundedReceiver<LogMessage>,
    log_by_key: HashMap<String, AsyncKafkaLog>,
    completed_polls: HashMap<NodeMsgId, Progress<(String, Vec<(usize, usize)>)>>,
    completed_commits: HashMap<NodeMsgId, Progress<String>>,
    completed_offset_reads: HashMap<NodeMsgId, Progress<(String, usize)>>,
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
            input_channel_rx: stdin_channel_rx,
            stdout_channel_tx,
            log_channel_tx,
            log_channel_rx,
            log_by_key: HashMap::new(),
            completed_polls: HashMap::new(),
            completed_commits: HashMap::new(),
            completed_offset_reads: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(input_msg) = self.input_channel_rx.recv() => {
                    match input_msg.body.payload {
                        KafkaLogOrKvPayload::KafkaLog(kafka_log_payload) => {
                            match kafka_log_payload {
                                KafkaLogPayload::Send { key, msg } => {
                                    self.send(
                                        input_msg.src,
                                        input_msg.body.msg_id.expect("msg_id is required (send)"),
                                        key,
                                        msg
                                    );
                                },
                                KafkaLogPayload::Poll { offsets } => {
                                    self.poll(
                                        input_msg.src,
                                        input_msg.body.msg_id.expect("msg_id is required (poll)"),
                                        offsets
                                    );
                                },
                                KafkaLogPayload::CommitOffsets { offsets } => {
                                    self.commit_offsets(
                                        input_msg.src,
                                        input_msg.body.msg_id.expect("msg_id is required (commit offsets)"),
                                        offsets
                                    )
                                },
                                KafkaLogPayload::ListCommittedOffsets { keys } => {
                                    self.list_committed_offsets(
                                        input_msg.src,
                                        input_msg.body.msg_id.expect("msg_id is required (list committed offsets)"),
                                        keys
                                    );
                                },
                                _ => {},
                            }
                        },
                        KafkaLogOrKvPayload::Kv(kv_payload) => {
                            let Some(in_reply_to) = input_msg.body.in_reply_to else { return; };
                            match kv_payload {
                                KvPayload::CasOk => {
                                    self.cas_ok(in_reply_to);
                                },
                                KvPayload::Error { code, text: _text } if code == KvErrorCode::CAS_ERROR => {
                                    self.cas_error(in_reply_to);
                                },
                                KvPayload::Error { code, text: _text } if code == KvErrorCode::KEY_NOT_FOUND => {
                                    self.key_not_found(in_reply_to);
                                },
                                KvPayload::ReadOk { value } => {
                                    self.read_ok(in_reply_to, value);
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

    fn send(&mut self, src: String, msg_id: usize, key: String, msg: usize) {
        let log = self.get_log_by_key_or_insert(key);
        let send_id = NodeMsgId::new(src, msg_id);
        log.send(send_id, msg);
    }

    fn poll(&mut self, src: String, msg_id: usize, offsets: HashMap<String, usize>) {
        let filtered_offsets = self.filter_offsets(offsets);

        let poll_id = NodeMsgId::new(src, msg_id);
        if filtered_offsets.len() == 0 {
            self.build_and_send_empty_poll_ok_msg(&poll_id);
            return;
        }

        self.completed_polls
            .insert(poll_id.clone(), Progress::new(filtered_offsets.len()));

        for (key, offset) in filtered_offsets {
            let log = self.log_by_key.get_mut(&key).unwrap();
            log.poll(poll_id.clone(), offset);
        }
    }

    fn commit_offsets(&mut self, src: String, msg_id: usize, offsets: HashMap<String, usize>) {
        let filtered_offsets = self.filter_offsets(offsets);

        let commit_id = NodeMsgId::new(src, msg_id);
        if filtered_offsets.len() == 0 {
            self.build_and_send_commit_offset_ok_msg(&commit_id);
            return;
        }

        self.completed_commits
            .insert(commit_id.clone(), Progress::new(filtered_offsets.len()));

        for (key, offset) in filtered_offsets {
            let log = self.log_by_key.get_mut(&key).unwrap();
            match log.commit_offset(commit_id.clone(), offset) {
                OperationStatus::CommitOffsetCompleted { commit_id } => {
                    self.commit_offset_completed(key, &commit_id);
                }
                OperationStatus::InProgress => {}
                OperationStatus::PollCompleted { .. } => {
                    panic!("received poll completed for commit_offsets");
                }
                OperationStatus::ListCommitOffsetCompleted { .. } => {
                    panic!("received list commit offset completed for commit_offsets");
                }
            };
        }
    }

    fn commit_offset_completed(&mut self, log_key: String, commit_id: &NodeMsgId) {
        let completed_commits = self.completed_commits.get_mut(&commit_id).unwrap();
        completed_commits.push(log_key);
        if completed_commits.is_completed() {
            self.completed_commits.remove(&commit_id);
            self.build_and_send_commit_offset_ok_msg(commit_id);
        }
    }

    fn list_committed_offsets(&mut self, src: String, msg_id: usize, log_keys: Vec<String>) {
        let filtered_log_keys = log_keys
            .into_iter()
            .filter(|key| self.log_by_key.contains_key(key))
            .collect::<Vec<_>>();

        let list_committed_offsets_id = NodeMsgId::new(src, msg_id);
        if filtered_log_keys.len() == 0 {
            self.build_and_send_empty_list_committed_offsets_ok_msg(&list_committed_offsets_id);
        } else {
            self.completed_offset_reads.insert(
                list_committed_offsets_id.clone(),
                Progress::new(filtered_log_keys.len()),
            );
            for key in filtered_log_keys {
                let log = self.log_by_key.get(&key).unwrap();
                log.read_committed_offset(list_committed_offsets_id.clone());
            }
        }
    }

    fn filter_offsets(&self, offsets: HashMap<String, usize>) -> HashMap<String, usize> {
        offsets
            .into_iter()
            .filter(|(key, _)| self.log_by_key.contains_key(key))
            .collect::<HashMap<_, _>>()
    }

    fn build_and_send_empty_poll_ok_msg(&self, poll_id: &NodeMsgId) {
        let poll_ok = self.build_empty_poll_ok_msg(poll_id);
        self.stdout_channel_tx
            .send(poll_ok)
            .expect("failed to write to seq-kv");
    }

    fn build_empty_poll_ok_msg(&self, poll_id: &NodeMsgId) -> Message<KafkaLogOrKvPayload> {
        self.build_poll_ok_msg(poll_id, HashMap::new())
    }

    fn build_and_send_poll_ok_msg(
        &self,
        poll_id: &NodeMsgId,
        msgs: impl Into<HashMap<String, Vec<(usize, usize)>>>,
    ) {
        let poll_ok = self.build_poll_ok_msg(poll_id, msgs);
        self.stdout_channel_tx
            .send(poll_ok)
            .expect("failed to write to seq-kv");
    }

    fn build_poll_ok_msg(
        &self,
        poll_id: &NodeMsgId,
        msgs: impl Into<HashMap<String, Vec<(usize, usize)>>>,
    ) -> Message<KafkaLogOrKvPayload> {
        Message {
            src: self.id.clone(),
            dst: poll_id.node_id.clone(),
            body: Body {
                msg_id: Some(self.msg_generator.generate_msg_id()),
                in_reply_to: Some(poll_id.msg_id),
                payload: KafkaLogOrKvPayload::KafkaLog(KafkaLogPayload::PollOk {
                    msgs: msgs.into(),
                }),
            },
        }
    }

    fn build_and_send_commit_offset_ok_msg(&self, commit_id: &NodeMsgId) {
        let commit_offset_ok = self.build_commit_offset_ok_msg(commit_id);
        self.stdout_channel_tx
            .send(commit_offset_ok)
            .expect("failed to send commit_offsets_ok");
    }

    fn build_commit_offset_ok_msg(&self, commit_id: &NodeMsgId) -> Message<KafkaLogOrKvPayload> {
        Message {
            src: self.id.clone(),
            dst: commit_id.node_id.clone(),
            body: Body {
                msg_id: None,
                in_reply_to: Some(commit_id.msg_id),
                payload: KafkaLogOrKvPayload::KafkaLog(KafkaLogPayload::CommitOffsetsOk),
            },
        }
    }

    fn build_and_send_empty_list_committed_offsets_ok_msg(
        &self,
        list_committed_offsets_id: &NodeMsgId,
    ) {
        let list_committed_offsets_ok =
            self.build_empty_list_committed_offsets_ok_msg(list_committed_offsets_id);
        self.stdout_channel_tx
            .send(list_committed_offsets_ok)
            .expect("failed to send list_committed_offsets_ok");
    }

    fn build_empty_list_committed_offsets_ok_msg(
        &self,
        list_committed_offsets_id: &NodeMsgId,
    ) -> Message<KafkaLogOrKvPayload> {
        self.build_list_committed_offsets_ok_msg(list_committed_offsets_id, HashMap::new())
    }

    fn build_and_send_list_committed_offsets_ok_msg(
        &self,
        list_committed_offsets_id: &NodeMsgId,
        offsets: HashMap<String, usize>,
    ) {
        let list_committed_offsets_ok =
            self.build_list_committed_offsets_ok_msg(list_committed_offsets_id, offsets);
        self.stdout_channel_tx
            .send(list_committed_offsets_ok)
            .expect("failed to send list_committed_offsets_ok");
    }

    fn build_list_committed_offsets_ok_msg(
        &self,
        list_committed_offsets_id: &NodeMsgId,
        offsets: HashMap<String, usize>,
    ) -> Message<KafkaLogOrKvPayload> {
        Message {
            src: self.id.clone(),
            dst: list_committed_offsets_id.node_id.clone(),
            body: Body {
                msg_id: None,
                in_reply_to: Some(list_committed_offsets_id.msg_id),
                payload: KafkaLogOrKvPayload::KafkaLog(KafkaLogPayload::ListCommittedOffsetsOk {
                    offsets,
                }),
            },
        }
    }

    fn build_and_send_send_ok_msg(&self, send_id: &NodeMsgId, offset: usize) {
        let send_ok = self.build_send_ok_msg(send_id, offset);
        self.stdout_channel_tx
            .send(send_ok)
            .expect("failed to write to seq-kv");
    }

    fn build_send_ok_msg(
        &self,
        send_id: &NodeMsgId,
        offset: usize,
    ) -> Message<KafkaLogOrKvPayload> {
        Message {
            src: self.id.clone(),
            dst: send_id.node_id.clone(),
            body: Body {
                msg_id: Some(self.msg_generator.generate_msg_id()),
                in_reply_to: Some(send_id.msg_id),
                payload: KafkaLogOrKvPayload::KafkaLog(KafkaLogPayload::SendOk { offset }),
            },
        }
    }

    fn get_log_by_key_or_insert(&mut self, key: String) -> &mut AsyncKafkaLog {
        self.log_by_key
            .entry(key.to_string())
            .or_insert(AsyncKafkaLog::new(
                key.to_string(),
                self.id.clone(),
                self.max_poll,
                self.log_channel_tx.clone(),
                self.stdout_channel_tx.clone(),
                Arc::clone(&self.msg_generator),
            ))
    }

    fn cas_ok(&mut self, in_reply_to: usize) {
        let node_id = self.id.clone();
        let (log_key, log) = self
            .consume_log_key(in_reply_to)
            .expect("cas_ok: log not found");

        let cas_id = NodeMsgId::new(node_id, in_reply_to);
        match log.cas_ok(&cas_id) {
            CasSemantics::SendCas { send_id, offset } => {
                self.cas_ok_send(&send_id, offset);
            }
            CasSemantics::CommitCas { commit_id, offset } => {
                self.cas_ok_commit(log_key, &commit_id, offset);
            }
        }
    }

    fn cas_ok_send(&self, send_id: &NodeMsgId, offset: usize) {
        self.build_and_send_send_ok_msg(&send_id, offset);
    }

    fn cas_ok_commit(&mut self, log_key: String, commit_id: &NodeMsgId, _offset: usize) {
        let commit_progress = self
            .completed_commits
            .get_mut(&commit_id)
            .expect("commit progress not found");
        commit_progress.push(log_key);

        if commit_progress.is_completed() {
            self.completed_commits.remove(&commit_id);
            self.stdout_channel_tx
                .send(Message {
                    src: self.id.clone(),
                    dst: commit_id.node_id.clone(),
                    body: Body {
                        msg_id: Some(self.msg_generator.generate_msg_id()),
                        in_reply_to: Some(commit_id.msg_id),
                        payload: KafkaLogOrKvPayload::KafkaLog(KafkaLogPayload::CommitOffsetsOk),
                    },
                })
                .expect("failed to write to seq-kv");
        }
    }

    fn consume_log_key(&mut self, msg_id: usize) -> Option<(String, &AsyncKafkaLog)> {
        self.msg_generator
            .consume_log_key(msg_id)
            .and_then(|log_key| self.log_by_key.get(&log_key).map(|log| (log_key, log)))
    }

    fn poll_completed(
        &mut self,
        poll_id: &NodeMsgId,
    ) -> Option<HashMap<String, Vec<(usize, usize)>>> {
        self.completed_polls
            .remove(&poll_id)
            .map(|poll_progress| poll_progress.into())
    }

    fn cas_error(&mut self, msg_id: usize) {
        let node_id = self.id.clone();
        let (_, log) = self
            .consume_log_key(msg_id)
            .expect("cas_error: log not found");
        log.cas_error(&NodeMsgId::new(node_id, msg_id));
    }

    fn key_not_found(&mut self, msg_id: usize) {
        let (log_key, log) = self
            .consume_log_key(msg_id)
            .expect("key_not_found: log not found");

        match log.missing_key_error(msg_id) {
            OperationStatus::PollCompleted { poll_id, msgs } => {
                self.poll_step_completed(log_key, msgs, &poll_id);
            }
            OperationStatus::CommitOffsetCompleted { .. } => {
                panic!("received commit offset completed for poll read missing key");
            }
            OperationStatus::ListCommitOffsetCompleted {
                list_committed_offset_id,
                offset,
            } => {
                self.list_committed_offsets_step_completed(
                    log_key,
                    &list_committed_offset_id,
                    offset,
                );
            }
            OperationStatus::InProgress => {}
        }
    }

    fn read_ok(&mut self, msg_id: usize, value: usize) {
        let (log_key, log) = self
            .consume_log_key(msg_id)
            .expect("read_ok: log not found");

        match log.read_ok(msg_id, value) {
            OperationStatus::PollCompleted { poll_id, msgs } => {
                self.poll_step_completed(log_key, msgs, &poll_id);
            }
            OperationStatus::CommitOffsetCompleted { commit_id } => {
                self.commit_offset_step_completed(log_key, &commit_id);
            }
            OperationStatus::ListCommitOffsetCompleted {
                list_committed_offset_id,
                offset,
            } => {
                self.list_committed_offsets_step_completed(
                    log_key,
                    &list_committed_offset_id,
                    offset,
                );
            }
            OperationStatus::InProgress => {}
        }
    }

    fn poll_step_completed(
        &mut self,
        log_key: String,
        msgs: Vec<(usize, usize)>,
        poll_id: &NodeMsgId,
    ) {
        let poll_progress = self
            .completed_polls
            .get_mut(&poll_id)
            .expect("poll progress not found");
        poll_progress.push((log_key, msgs));

        if poll_progress.is_completed() {
            let msgs = self.poll_completed(&poll_id).expect("poll_id not found");
            self.build_and_send_poll_ok_msg(&poll_id, msgs);
        }
    }

    fn commit_offset_step_completed(&mut self, log_key: String, commit_id: &NodeMsgId) {
        let commit_progress = self
            .completed_commits
            .get_mut(&commit_id)
            .expect("commit progress not found");

        commit_progress.push(log_key);
        if commit_progress.is_completed() {
            self.completed_commits.remove(&commit_id);
            self.build_and_send_commit_offset_ok_msg(&commit_id);
        }
    }

    fn list_committed_offsets_step_completed(
        &mut self,
        log_key: String,
        list_committed_offset_id: &NodeMsgId,
        offset: usize,
    ) {
        let list_committed_offset_progress = self
            .completed_offset_reads
            .get_mut(&list_committed_offset_id)
            .expect("list_committed_offset_progress not found");

        list_committed_offset_progress.push((log_key, offset));
        if list_committed_offset_progress.is_completed() {
            let list_committed_offset_progress = self
                .completed_offset_reads
                .remove(&list_committed_offset_id)
                .unwrap();
            self.build_and_send_list_committed_offsets_ok_msg(
                list_committed_offset_id,
                list_committed_offset_progress.into(),
            );
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
            LogMsgSemantics::ReadUpdatedCommittedOffset { .. } => {
                panic!("cas_ok called for ReadUpdatedCommittedOffset")
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
        match self
            .semantics_by_msg_id
            .remove(cas_id)
            .expect(&format!("cas_error called for unknown msg_id {:?}", cas_id))
            .1
        {
            LogMsgSemantics::CasSend {
                send_id,
                msg,
                offset,
            } => {
                self.cas_error_send(send_id, msg, offset);
            }
            LogMsgSemantics::CasCommitOffset { commit_id, offset } => {
                self.cas_error_commit_offset(commit_id, offset);
            }
            LogMsgSemantics::ReadUpdatedOffset { .. } => {
                panic!("cas_error called for ReadUpdateOffset");
            }
            LogMsgSemantics::ReadPollMessage { .. } => {
                panic!("cas_error called for ReadPollMessage");
            }
            LogMsgSemantics::ReadUpdatedCommittedOffset { .. } => {
                panic!("cas_error called for ReadUpdatedCommittedOffset");
            }
        }
    }

    fn cas_error_send(&self, send_id: NodeMsgId, msg: usize, offset: usize) {
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

    fn cas_error_commit_offset(&self, commit_id: NodeMsgId, offset: usize) {
        let read_msg_id = self.msg_generator.generate_log_msg_id(self.key.clone());
        self.stdout_channel_tx
            .send(Message {
                src: self.node_id.clone(),
                dst: Self::LIN_KV.to_string(),
                body: Body {
                    msg_id: Some(read_msg_id),
                    in_reply_to: None,
                    payload: KafkaLogOrKvPayload::Kv(KvPayload::Read {
                        key: self.committed_offset_key(),
                    }),
                },
            })
            .expect("failed to read from lin-kv");

        self.semantics_by_msg_id.insert(
            NodeMsgId::new(self.node_id.clone(), read_msg_id),
            LogMsgSemantics::ReadUpdatedCommittedOffset {
                commit_id,
                offset: Some(offset),
            },
        );
    }

    fn read_ok(&self, read_msg_id: usize, value: usize) -> OperationStatus {
        let read_semantics = match self
            .semantics_by_msg_id
            .get(&NodeMsgId::new(self.node_id.clone(), read_msg_id))
        {
            None => panic!("read_ok called for unknown msg_id {}", read_msg_id),
            Some(entry) => match entry.value() {
                LogMsgSemantics::ReadUpdatedOffset { .. } => ReadOkSemantics::ReadUpdatedOffset,
                LogMsgSemantics::ReadPollMessage { .. } => ReadOkSemantics::ReadPollMessage,
                LogMsgSemantics::ReadUpdatedCommittedOffset { commit_id, offset }
                    if offset.is_some() =>
                {
                    ReadOkSemantics::ReadUpdatedCommittedOffset
                }
                LogMsgSemantics::ReadUpdatedCommittedOffset { commit_id, offset } => {
                    ReadOkSemantics::ListCommittedOffset
                }
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
            ReadOkSemantics::ReadUpdatedOffset => self.update_offset_read_ok(read_msg_id, value),
            ReadOkSemantics::ReadPollMessage => self.poll_read_ok(read_msg_id, value),
            ReadOkSemantics::ReadUpdatedCommittedOffset => {
                self.read_updated_committed_offset_read_ok(read_msg_id, value)
            }
            ReadOkSemantics::ListCommittedOffset => {
                self.list_committed_offset_read_ok(read_msg_id, value)
            }
        }
    }

    fn list_committed_offset_read_ok(
        &self,
        read_msg_id: usize,
        new_offset: usize,
    ) -> OperationStatus {
        let LogMsgSemantics::ReadUpdatedCommittedOffset {
            commit_id: list_committed_offset_id,
            offset: None,
        } = self
            .semantics_by_msg_id
            .remove(&NodeMsgId::new(self.node_id.clone(), read_msg_id))
            .expect(&format!(
                "list_committed_offset_read_ok called for unknown msg_id {}",
                read_msg_id
            ))
            .1
        else {
            panic!(
                "list_committed_offset_read_ok called for unknown msg_id {}",
                read_msg_id
            )
        };
        self.local_committed_offset
            .store(new_offset, Ordering::Relaxed);
        OperationStatus::ListCommitOffsetCompleted {
            list_committed_offset_id,
            offset: new_offset,
        }
    }

    fn read_updated_committed_offset_read_ok(
        &self,
        read_msg_id: usize,
        new_offset: usize,
    ) -> OperationStatus {
        let LogMsgSemantics::ReadUpdatedCommittedOffset {
            commit_id,
            offset: Some(offset),
        } = self
            .semantics_by_msg_id
            .remove(&NodeMsgId::new(self.node_id.clone(), read_msg_id))
            .expect(&format!(
                "update_committed_offset called for unknown msg_id {}",
                read_msg_id
            ))
            .1
        else {
            panic!(
                "update_committed_offset called for unknown msg_id {}",
                read_msg_id
            )
        };
        self.local_committed_offset
            .store(new_offset, Ordering::Relaxed);
        self.commit_offset(commit_id, offset)
    }

    fn update_offset_read_ok(&self, read_msg_id: usize, new_offset: usize) -> OperationStatus {
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

    fn missing_key_error(&self, read_msg_id: usize) -> OperationStatus {
        match self
            .semantics_by_msg_id
            .remove(&NodeMsgId::new(self.node_id.clone(), read_msg_id))
            .expect("missing_key_error called for unknown msg_id")
            .1
        {
            LogMsgSemantics::ReadUpdatedOffset { .. } => {
                panic!("missing_key_error called for ReadUpdateOffset");
            }
            LogMsgSemantics::ReadPollMessage {
                poll_id,
                offset: _offset,
            } => self.poll_read_missing_key(poll_id),
            LogMsgSemantics::ReadUpdatedCommittedOffset {
                commit_id,
                offset: _offset,
            } => OperationStatus::ListCommitOffsetCompleted {
                list_committed_offset_id: commit_id,
                offset: 0,
            },
            LogMsgSemantics::CasSend { .. } => {
                panic!("missing_key_error called for CasSend");
            }
            LogMsgSemantics::CasCommitOffset { .. } => {
                panic!("missing_key_error called for CasCommitOffset");
            }
        }
    }

    fn poll_read_missing_key(&self, poll_id: NodeMsgId) -> OperationStatus {
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
            return OperationStatus::CommitOffsetCompleted { commit_id };
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

    fn read_committed_offset(&self, list_commit_offset_id: NodeMsgId) {
        let read_msg_id = self.msg_generator.generate_log_msg_id(self.key.clone());
        self.semantics_by_msg_id.insert(
            NodeMsgId::new(self.node_id.clone(), read_msg_id),
            LogMsgSemantics::ReadUpdatedCommittedOffset {
                commit_id: list_commit_offset_id,
                offset: None,
            },
        );

        self.stdout_channel_tx
            .send(Message {
                src: self.node_id.clone(),
                dst: Self::LIN_KV.to_string(),
                body: Body {
                    msg_id: Some(read_msg_id),
                    in_reply_to: None,
                    payload: KafkaLogOrKvPayload::Kv(KvPayload::Read {
                        key: self.committed_offset_key(),
                    }),
                },
            })
            .expect("failed to read from lin-kv");
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
    CommitOffsetCompleted {
        commit_id: NodeMsgId,
    },
    ListCommitOffsetCompleted {
        list_committed_offset_id: NodeMsgId,
        offset: usize,
    },
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
    ReadUpdatedCommittedOffset {
        commit_id: NodeMsgId,
        offset: Option<usize>,
    },
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
enum ReadOkSemantics {
    ReadUpdatedOffset,
    ReadPollMessage,
    ReadUpdatedCommittedOffset,
    ListCommittedOffset,
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

impl Deref for Progress<(String, usize)> {
    type Target = Vec<(String, usize)>;
    fn deref(&self) -> &Self::Target {
        &self.progress
    }
}

impl DerefMut for Progress<(String, usize)> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.progress
    }
}

impl From<HashMap<String, usize>> for Progress<(String, usize)> {
    fn from(value: HashMap<String, usize>) -> Self {
        Self {
            tot_log_keys: value.len(),
            progress: value.into_iter().collect(),
        }
    }
}

impl From<Progress<(String, usize)>> for HashMap<String, usize> {
    fn from(value: Progress<(String, usize)>) -> Self {
        value.progress.into_iter().collect()
    }
}

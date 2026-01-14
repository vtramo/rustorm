use crate::Message;
use crate::node::{Node, common_init_node};
use crate::payloads::{Event, InitPayload, KafkaLogPayload};
use crate::stdout_json::StdoutJson;
use std::collections::HashMap;
use std::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct KafkaLogNode {
    _id: String,
    msg_id: usize,
    logs: HashMap<String, KafkaLog>,
}

impl Node<KafkaLogPayload, ()> for KafkaLogNode {
    fn init(
        init_msg: Message<InitPayload>,
        output: &mut StdoutJson,
        _tx_channel: Sender<Event<KafkaLogPayload, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (node_id, _node_ids) = common_init_node(init_msg, output)?;
        Ok(Self {
            _id: node_id,
            msg_id: 0,
            logs: HashMap::new(),
        })
    }

    fn step(
        &mut self,
        event: Event<KafkaLogPayload, ()>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        match event {
            Event::Message(msg) => {
                let mut reply = msg.into_reply(Some(&mut self.msg_id));
                match reply.body.payload {
                    KafkaLogPayload::Send { key, msg } => {
                        let kafka_log = self
                            .logs
                            .entry(key)
                            .or_insert_with(|| KafkaLog::new(5, 100));

                        let offset = kafka_log.append(msg);
                        reply.body.payload = KafkaLogPayload::SendOk { offset };
                        output.write(&reply)?;
                    }
                    KafkaLogPayload::SendOk { .. } => {}
                    KafkaLogPayload::Poll { offsets } => {
                        let mut msgs: HashMap<String, Vec<(usize, usize)>> =
                            HashMap::with_capacity(offsets.len());

                        for (key, offset) in offsets {
                            if let Some(log) = self.logs.get(&key) {
                                msgs.insert(
                                    key,
                                    log.poll(offset)
                                        .iter()
                                        .map(|(offset, msg)| (*offset, *msg))
                                        .collect(),
                                );
                            }
                        }

                        reply.body.payload = KafkaLogPayload::PollOk { msgs };
                        output.write(&reply)?;
                    }
                    KafkaLogPayload::PollOk { .. } => {}
                    KafkaLogPayload::CommitOffsets { offsets } => {
                        for (key, offset) in offsets {
                            if let Some(log) = self.logs.get_mut(&key) {
                                log.commit_offset(offset);
                            }
                        }

                        reply.body.payload = KafkaLogPayload::CommitOffsetsOk;
                        output.write(&reply)?;
                    }
                    KafkaLogPayload::CommitOffsetsOk => {}
                    KafkaLogPayload::ListCommittedOffsets { keys } => {
                        let mut offsets: HashMap<String, usize> =
                            HashMap::with_capacity(keys.len());

                        for key in keys {
                            if let Some(log) = self.logs.get(&key) {
                                offsets.insert(key, log.get_committed_offset());
                            }
                        }

                        reply.body.payload = KafkaLogPayload::ListCommittedOffsetsOk { offsets };
                        output.write(&reply)?;
                    }
                    KafkaLogPayload::ListCommittedOffsetsOk { .. } => {}
                }
            }
            Event::InjectedPayload(_) => {}
        };

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct KafkaLog {
    max_poll: usize,
    committed_up_to: usize,
    messages: Vec<usize>,
}

impl KafkaLog {
    fn new(max_poll: usize, start_capacity: usize) -> Self {
        Self {
            max_poll,
            committed_up_to: 0,
            messages: Vec::with_capacity(start_capacity),
        }
    }

    fn append(&mut self, msg: usize) -> usize {
        let offset = self.messages.len();
        self.messages.push(msg);
        offset
    }

    fn poll(&self, start_offset: usize) -> Vec<(usize, usize)> {
        if start_offset >= self.messages.len() {
            return vec![];
        }

        let end = std::cmp::min(self.messages.len(), start_offset + self.max_poll);
        self.messages[start_offset..end]
            .iter()
            .enumerate()
            .map(|(i, &msg)| {
                (start_offset + i, msg)
            })
            .collect()
    }

    fn commit_offset(&mut self, offset: usize) {
        if offset < self.messages.len() {
            self.committed_up_to = offset;
        }
    }

    fn get_committed_offset(&self) -> usize {
        self.committed_up_to
    }
}

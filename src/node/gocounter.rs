use crate::node::{Node, common_init_node};
use crate::payloads::SeqKvPayload::Write;
use crate::payloads::{
    Event, GoCounterOrSeqKvPayload, GoCounterPayload, InitPayload, SeqKvPayload, SyncCounter,
};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct GrowOnlyCounterNode {
    pub id: String,
    pub msg_id: usize,
    pub counter: usize,
    pub node_ids: Vec<String>,
    pub value_by_node_id: HashMap<String, usize>,
    pub node_id_by_msg_id: HashMap<usize, String>,
}

impl Node<GoCounterOrSeqKvPayload, SyncCounter> for GrowOnlyCounterNode {
    fn init(
        init_msg: Message<InitPayload>,
        output: &mut StdoutJson,
        tx_channel: std::sync::mpsc::Sender<Event<GoCounterOrSeqKvPayload, SyncCounter>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (this_node_id, node_ids) = common_init_node(init_msg, output)?;
        let node_ids = node_ids
            .into_iter()
            .filter(|node_id| *node_id != this_node_id)
            .collect::<Vec<_>>();
        let multi_node_broadcast = Self {
            id: this_node_id,
            msg_id: 0,
            counter: 0,
            value_by_node_id: node_ids
                .iter()
                .cloned()
                .map(|node_id| (node_id, 0))
                .collect(),
            node_ids,
            node_id_by_msg_id: HashMap::new(),
        };
        Self::spawn_sync_counter_thread(tx_channel);
        Ok(multi_node_broadcast)
    }

    fn step(
        &mut self,
        event: Event<GoCounterOrSeqKvPayload, SyncCounter>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        match event {
            Event::Message(message) => {
                let in_reply_to = message.body.in_reply_to;
                let mut reply = message.into_reply(Some(&mut self.msg_id));
                match reply.body.payload {
                    GoCounterOrSeqKvPayload::GoCounter(go_counter_payload) => {
                        match go_counter_payload {
                            GoCounterPayload::Read => {
                                let global_value: usize =
                                    self.counter + self.value_by_node_id.values().sum::<usize>();
                                reply.body.payload =
                                    GoCounterOrSeqKvPayload::GoCounter(GoCounterPayload::ReadOk {
                                        value: global_value,
                                    });
                                output.write(&reply)?;
                            }
                            GoCounterPayload::Add { delta } => {
                                self.counter += delta;
                                reply.body.payload =
                                    GoCounterOrSeqKvPayload::GoCounter(GoCounterPayload::AddOk);
                                output.write(&reply)?;

                                let msg_id = self.msg_id;
                                self.msg_id += 1;
                                let write = Message {
                                    src: self.id.clone(),
                                    dst: "seq-kv".to_string(),
                                    body: Body {
                                        msg_id: Some(msg_id),
                                        in_reply_to: None,
                                        payload: GoCounterOrSeqKvPayload::SeqKv(Write {
                                            key: self.id.clone(),
                                            value: self.counter,
                                        }),
                                    },
                                };
                                output.write(&write)?;
                            }
                            GoCounterPayload::ReadOk { .. } | GoCounterPayload::AddOk { .. } => {}
                        }
                    }
                    GoCounterOrSeqKvPayload::SeqKv(seq_kv_payload) => match seq_kv_payload {
                        SeqKvPayload::ReadOk { value } => {
                            let Some(in_reply_to) = in_reply_to else {
                                return Ok(());
                            };
                            let Some(node_id) = self.node_id_by_msg_id.remove(&in_reply_to) else {
                                return Ok(());
                            };
                            self.value_by_node_id
                                .entry(node_id)
                                .and_modify(|v| *v = (*v).max(value))
                                .or_insert(value);
                        }
                        Write { .. }
                        | SeqKvPayload::CasOk
                        | SeqKvPayload::WriteOk
                        | SeqKvPayload::Cas { .. }
                        | SeqKvPayload::Read { .. } => {}
                    },
                }
            }
            Event::InjectedPayload(_sync_msg) => {
                for node_id in &self.node_ids {
                    let msg_id = self.msg_id;
                    self.msg_id += 1;

                    self.node_id_by_msg_id.insert(msg_id, node_id.clone());

                    let seq_kv_read = Message {
                        src: self.id.clone(),
                        dst: "seq-kv".to_string(),
                        body: Body {
                            msg_id: Some(msg_id),
                            in_reply_to: None,
                            payload: GoCounterOrSeqKvPayload::SeqKv(SeqKvPayload::Read {
                                key: node_id.clone(),
                            }),
                        },
                    };

                    output.write(&seq_kv_read)?;
                }
            }
        }

        Ok(())
    }
}

impl GrowOnlyCounterNode {
    fn spawn_sync_counter_thread(
        tx_channel: std::sync::mpsc::Sender<Event<GoCounterOrSeqKvPayload, SyncCounter>>,
    ) {
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(1000));
                if let Err(_) = tx_channel.send(Event::InjectedPayload(SyncCounter::Sync)) {
                    break;
                }
            }
        });
    }
}

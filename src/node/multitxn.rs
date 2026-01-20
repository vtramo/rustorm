use crate::node::{Node, common_init_node};
use crate::payloads::{Event, InitPayload, TxnOperation, TxnPayload};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};
use std::collections::HashMap;
use std::sync::mpsc::Sender;

#[derive(Debug)]
pub struct MultiTxnNode {
    id: String,
    msg_id: usize,
    node_ids: Vec<String>,
    map: HashMap<usize, usize>,
}

impl Node<TxnPayload, ()> for MultiTxnNode {
    fn init(
        init_msg: Message<InitPayload>,
        output: &mut StdoutJson,
        _tx_channel: Sender<Event<TxnPayload, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (node_id, node_ids) = common_init_node(init_msg, output)?;
        Ok(Self {
            id: node_id,
            msg_id: 0,
            node_ids,
            map: HashMap::new(),
        })
    }

    fn step(
        &mut self,
        event: Event<TxnPayload, ()>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        match event {
            Event::Message(msg) => {
                let mut reply = msg.into_reply(Some(&mut self.msg_id));
                let TxnPayload::Txn { txn } = reply.body.payload else {
                    return Ok(());
                };
                let txn_reply = self.process_txn(txn);
                self.communicate_txn(&txn_reply, output)?;
                reply.body.payload = TxnPayload::TxnOk { txn: txn_reply };
                output.write(&reply)?;
            }
            Event::InjectedPayload(_) => {}
        };

        Ok(())
    }
}

impl MultiTxnNode {
    fn process_txn(&mut self, txn: Vec<TxnOperation>) -> Vec<TxnOperation> {
        let mut txn_reply = Vec::with_capacity(txn.len());
        for txn_operation in txn {
            match txn_operation {
                TxnOperation::Read { key, value: _ } => {
                    let Some(value) = self.map.get(&key) else {
                        continue;
                    };
                    txn_reply.push(TxnOperation::Read {
                        key,
                        value: Some(*value),
                    });
                }
                TxnOperation::Write { key, value } => {
                    self.map.insert(key, value);
                    txn_reply.push(TxnOperation::Write { key, value });
                }
            }
        }
        txn_reply
    }

    fn communicate_txn(
        &mut self,
        txn: &Vec<TxnOperation>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        for node_id in &self.node_ids {
            if node_id == &self.id {
                continue;
            }

            let txn_msg = Message {
                src: self.id.clone(),
                dst: node_id.clone(),
                body: Body {
                    msg_id: Some(self.msg_id),
                    in_reply_to: None,
                    payload: TxnPayload::Txn {
                        txn: txn
                            .into_iter()
                            .cloned()
                            .filter(|op| !matches!(op, TxnOperation::Write { .. }))
                            .collect(),
                    },
                },
            };

            output.write(&txn_msg)?;

            self.msg_id += 1;
        }

        Ok(())
    }
}

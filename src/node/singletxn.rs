use crate::Message;
use crate::node::{Node, common_init_node};
use crate::payloads::{Event, InitPayload, TxnOperation, TxnPayload};
use crate::stdout_json::StdoutJson;
use std::collections::HashMap;
use std::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct SingleTxnNode {
    _id: String,
    msg_id: usize,
    map: HashMap<usize, usize>,
}

impl Node<TxnPayload, ()> for SingleTxnNode {
    fn init(
        init_msg: Message<InitPayload>,
        output: &mut StdoutJson,
        _tx_channel: Sender<Event<TxnPayload, ()>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (node_id, _) = common_init_node(init_msg, output)?;
        Ok(Self {
            _id: node_id,
            msg_id: 0,
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
                    panic!("invalid payload")
                };

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

                reply.body.payload = TxnPayload::TxnOk { txn: txn_reply };
                output.write(&reply)?;
            }
            Event::InjectedPayload(_) => {}
        };

        Ok(())
    }
}

use crate::node::{Node, common_init_node};
use crate::payloads::{BroadcastPayload, Event, InitPayload, InjectedPayload};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct MultiNodeBroadcast {
    pub id: String,
    pub msg_id: usize,
    pub broadcast_messages: HashSet<usize>,
    pub adj: HashMap<String, Vec<String>>,
}

impl Node<BroadcastPayload, InjectedPayload> for MultiNodeBroadcast {
    fn init(
        init_msg: Message<InitPayload>,
        output: &mut StdoutJson,
        tx_channel: std::sync::mpsc::Sender<Event<BroadcastPayload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (node_id, node_ids) = common_init_node(init_msg, output)?;
        let multi_node_broadcast = Self {
            id: node_id,
            msg_id: 0,
            broadcast_messages: HashSet::new(),
            adj: HashMap::with_capacity(node_ids.len()),
        };
        Self::generate_gossiping_thread(tx_channel);
        Ok(multi_node_broadcast)
    }

    fn step(
        &mut self,
        event: Event<BroadcastPayload, InjectedPayload>,
        output: &mut StdoutJson,
    ) -> anyhow::Result<()> {
        match event {
            Event::Message(message) => {
                let mut reply = message.into_reply(Some(&mut self.msg_id));
                match reply.body.payload {
                    BroadcastPayload::Broadcast { message } => {
                        self.broadcast_messages.insert(message);
                        reply.body.payload = BroadcastPayload::BroadcastOk;
                        output.write(&reply)?;
                    }
                    BroadcastPayload::Read => {
                        reply.body.payload = BroadcastPayload::ReadOk {
                            messages: self.broadcast_messages.clone(),
                        };
                        output.write(&reply)?;
                    }
                    BroadcastPayload::Topology { mut topology } => {
                        self.adj = topology
                            .remove(&self.id)
                            .expect(&format!("topology for node {} not received!", self.id))
                            .into_iter()
                            .filter(|adj_node_id| adj_node_id != &self.id)
                            .map(|adj_node_id| (adj_node_id, Vec::new()))
                            .collect();
                        reply.body.payload = BroadcastPayload::TopologyOk;
                        output.write(&reply)?;
                    }
                    BroadcastPayload::Gossip { seen } => {
                        self.broadcast_messages.extend(seen);
                    }
                    BroadcastPayload::TopologyOk { .. }
                    | BroadcastPayload::ReadOk { .. }
                    | BroadcastPayload::BroadcastOk
                    | BroadcastPayload::GossipOk { .. } => {}
                };
            }
            Event::InjectedPayload(injected_payload) => match injected_payload {
                InjectedPayload::Gossip => {
                    for adj_node_id in self.adj.keys() {
                        let gossip_msg = Message {
                            src: self.id.clone(),
                            dst: adj_node_id.clone(),
                            body: Body {
                                msg_id: None,
                                in_reply_to: None,
                                payload: BroadcastPayload::Gossip {
                                    seen: self.broadcast_messages.clone(),
                                },
                            },
                        };
                        output.write(&gossip_msg)?;
                    }
                }
            },
        }

        Ok(())
    }
}

impl MultiNodeBroadcast {
    fn generate_gossiping_thread(
        tx_channel: std::sync::mpsc::Sender<Event<BroadcastPayload, InjectedPayload>>,
    ) {
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(300));
                if let Err(_) = tx_channel.send(Event::InjectedPayload(InjectedPayload::Gossip)) {
                    break;
                }
            }
        });
    }
}

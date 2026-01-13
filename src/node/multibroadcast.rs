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
    pub adj: HashMap<String, HashSet<usize>>,
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
                let src = message.src.clone();
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
                        self.adj = self.construct_topology(&mut topology);
                        reply.body.payload = BroadcastPayload::TopologyOk;
                        output.write(&reply)?;
                    }
                    BroadcastPayload::Gossip { seen } => {
                        self.broadcast_messages.extend(seen.clone());
                        if let Some(adj_seen) = self.adj.get_mut(&src) {
                            adj_seen.extend(seen);
                        }
                        reply.body.payload = BroadcastPayload::GossipOk {
                            seen: self.broadcast_messages.clone(),
                        };
                    }
                    BroadcastPayload::GossipOk { seen } => {
                        self.broadcast_messages.extend(seen.clone());
                        if let Some(adj_seen) = self.adj.get_mut(&src) {
                            adj_seen.extend(seen);
                        }
                    }
                    BroadcastPayload::TopologyOk { .. }
                    | BroadcastPayload::ReadOk { .. }
                    | BroadcastPayload::BroadcastOk => {}
                };
            }
            Event::InjectedPayload(injected_payload) => match injected_payload {
                InjectedPayload::Gossip => {
                    for adj_node_id in self.adj.keys() {
                        if self.is_fully_synced(adj_node_id) {
                            continue;
                        }

                        let gossip_msg = Message {
                            src: self.id.clone(),
                            dst: adj_node_id.clone(),
                            body: Body {
                                msg_id: None,
                                in_reply_to: None,
                                payload: BroadcastPayload::Gossip {
                                    seen: self.get_unseen_messages(adj_node_id).collect(),
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

    fn construct_topology(
        &mut self,
        topology: &mut HashMap<String, Vec<String>>,
    ) -> HashMap<String, HashSet<usize>> {
        topology
            .remove(&self.id)
            .expect(&format!("topology for node {} not received!", self.id))
            .into_iter()
            .filter(|adj_node_id| adj_node_id != &self.id)
            .map(|adj_node_id| (adj_node_id, HashSet::new()))
            .collect()
    }

    fn is_fully_synced(&self, adj: impl AsRef<str>) -> bool {
        let seen_by_adj = &self.adj[adj.as_ref()];
        seen_by_adj.len() == self.broadcast_messages.len()
    }

    fn get_unseen_messages(&self, adj: impl AsRef<str>) -> impl Iterator<Item = usize> {
        let seen_by_adj = &self.adj[adj.as_ref()];
        self.broadcast_messages
            .iter()
            .filter(|msg| !seen_by_adj.contains(*msg))
            .copied()
    }
}

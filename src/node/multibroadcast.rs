use crate::node::{common_init_node, Node};
use crate::payloads::{BroadcastPayload, Event, InitPayload, InjectedPayload};
use crate::stdout_json::StdoutJson;
use crate::{Body, Message};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct MultiNodeBroadcast {
    pub id: String,
    pub msg_id: usize,
    pub broadcast_messages: HashSet<usize>,
    pub adj: HashSet<String>,
    pub known: HashMap<String, HashSet<usize>>,
    pub msg_communicated: HashMap<usize, HashSet<usize>>,
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
            adj: HashSet::with_capacity(node_ids.len()),
            known: node_ids
                .into_iter()
                .map(|id| (id, HashSet::new()))
                .collect(),
            msg_communicated: Default::default(),
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
                let in_reply_to = message.body.in_reply_to;
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
                        self.adj = self.construct_topology(&mut topology).collect();
                        reply.body.payload = BroadcastPayload::TopologyOk;
                        output.write(&reply)?;
                    }
                    BroadcastPayload::Gossip { seen } => {
                        self.broadcast_messages.extend(seen.clone());
                        if let Some(adj_seen) = self.known.get_mut(&src) {
                            adj_seen.extend(seen);
                        }
                        reply.body.payload = BroadcastPayload::GossipOk;
                        output.write(&reply)?;
                    }
                    BroadcastPayload::GossipOk => {
                        if let Some((adj_seen, msg_communicated)) =
                            self.known.get_mut(&src).zip(self.msg_communicated.get_mut(
                                &in_reply_to.expect("gossip ok should have in_reply_to field"),
                            ))
                        {
                            adj_seen.extend(msg_communicated.clone());
                        }
                    }
                    BroadcastPayload::TopologyOk { .. }
                    | BroadcastPayload::ReadOk { .. }
                    | BroadcastPayload::BroadcastOk => {}
                };
            }
            Event::InjectedPayload(injected_payload) => match injected_payload {
                InjectedPayload::Gossip => {
                    for adj_node_id in &self.adj {
                        if self.is_fully_synced(adj_node_id) {
                            continue;
                        }

                        let unseen_messages = self
                            .get_unseen_messages(adj_node_id)
                            .collect::<HashSet<_>>();
                        let gossip_msg = Message {
                            src: self.id.clone(),
                            dst: adj_node_id.clone(),
                            body: Body {
                                msg_id: Some(self.msg_id),
                                in_reply_to: None,
                                payload: BroadcastPayload::Gossip {
                                    seen: unseen_messages.clone(),
                                },
                            },
                        };

                        self.msg_communicated.insert(self.msg_id, unseen_messages);
                        self.msg_id += 1;

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
    ) -> impl Iterator<Item = String> {
        topology
            .remove(&self.id)
            .expect(&format!("topology for node {} not received!", self.id))
            .into_iter()
            .filter(|adj_node_id| adj_node_id != &self.id)
    }

    fn is_fully_synced(&self, adj: impl AsRef<str>) -> bool {
        let seen_by_adj = &self.known[adj.as_ref()];
        seen_by_adj.len() == self.broadcast_messages.len()
    }

    fn get_unseen_messages(&self, adj: impl AsRef<str>) -> impl Iterator<Item = usize> {
        let seen_by_adj = &self.known[adj.as_ref()];
        self.broadcast_messages
            .iter()
            .filter(|msg| !seen_by_adj.contains(*msg))
            .copied()
    }
}

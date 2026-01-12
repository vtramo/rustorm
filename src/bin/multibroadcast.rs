use rustorm::mloop::main_loop;
use rustorm::node::multibroadcast::MultiNodeBroadcast;
use rustorm::payloads::{BroadcastPayload, InjectedPayload};

fn main() -> anyhow::Result<()> {
    main_loop::<MultiNodeBroadcast, BroadcastPayload, InjectedPayload>()
}

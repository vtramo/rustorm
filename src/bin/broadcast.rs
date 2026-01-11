use rustorm::mloop::main_loop;
use rustorm::node::broadcast::BroadcastNode;
use rustorm::payloads::BroadcastPayload;

fn main() -> anyhow::Result<()> {
    main_loop::<BroadcastNode, BroadcastPayload>()
}
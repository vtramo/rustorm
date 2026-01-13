use rustorm::mloop::main_loop;
use rustorm::node::gocounter::GrowOnlyCounterNode;
use rustorm::payloads::{GoCounterOrSeqKvPayload, SyncCounter};

fn main() -> anyhow::Result<()> {
    main_loop::<GrowOnlyCounterNode, GoCounterOrSeqKvPayload, SyncCounter>()
}

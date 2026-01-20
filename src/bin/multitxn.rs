use rustorm::mloop::main_loop;
use rustorm::node::multitxn::MultiTxnNode;
use rustorm::payloads::{TxnPayload};

fn main() -> anyhow::Result<()> {
    main_loop::<MultiTxnNode, TxnPayload, ()>()?;
    Ok(())
}

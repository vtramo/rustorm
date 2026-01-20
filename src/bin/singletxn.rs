use rustorm::mloop::main_loop;
use rustorm::node::singletxn::SingleTxnNode;
use rustorm::payloads::{TxnPayload};

fn main() -> anyhow::Result<()> {
    main_loop::<SingleTxnNode, TxnPayload, ()>()?;
    Ok(())
}

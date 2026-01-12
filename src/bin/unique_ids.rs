use rustorm::mloop::main_loop;
use rustorm::node::generate::GenerateNode;
use rustorm::payloads::GeneratePayload;

fn main() -> anyhow::Result<()> {
    main_loop::<GenerateNode, GeneratePayload, ()>()?;
    Ok(())
}

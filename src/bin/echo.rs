use rustorm::mloop::main_loop;
use rustorm::node::echo::EchoNode;
use rustorm::payloads::EchoPayload;

fn main() -> anyhow::Result<()> {
    main_loop::<EchoNode, EchoPayload>()    
}
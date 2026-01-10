use anyhow::Context;
use rustorm::{EchoNode, Message};
use rustorm::stdout_json::StdoutJson;

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout_json = StdoutJson::new();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut echo_node = EchoNode {
        msg_id: 0,
        id: None,
    };

    for input in inputs {
        let input = input.context("Maelstrom input from STDIN could not be deserialized")?;
        echo_node
            .step(input, &mut stdout_json)
            .context("Node step function failed")?;
    }

    Ok(())
}

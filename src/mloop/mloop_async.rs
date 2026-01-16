use crate::Message;
use crate::mloop::init;
use crate::payloads::{KafkaLogOrKvPayload, KafkaLogPayload};
use crate::stdout_json::StdoutJson;
use tokio::io::AsyncReadExt;
use crate::node::multikafkalog::MultiKafkaLogNode;

pub async fn main_loop_async() -> anyhow::Result<()> {
    let init_msg = init()?;

    let (stdout_tx, mut stdout_rx) =
        tokio::sync::mpsc::unbounded_channel::<Message<KafkaLogOrKvPayload>>();
    let (stdin_tx, stdin_rx) = tokio::sync::mpsc::unbounded_channel::<Message<KafkaLogOrKvPayload>>();

    tokio::spawn(async move {
        let mut async_stdin = tokio::io::stdin();
        let mut stdout = StdoutJson::new();

        let mut stdin_buf = [0u8; 1024];
        tokio::select! {
            Some(msg) = stdout_rx.recv() => {
                stdout.write(&msg).expect("failed to write to stdout");
            },
            Ok(_) = async_stdin.read(&mut stdin_buf) => {
                let msg = serde_json::from_slice::<Message<KafkaLogOrKvPayload>>(&stdin_buf)
                    .expect("msg deserialization failed");
                stdin_tx.send(msg).expect("failed to send msg to stdout channel");
                stdin_buf.fill(0);
            },
        }
    });

    let mut node = MultiKafkaLogNode::new(
        init_msg.body.payload.node_id,
        stdin_rx,
        stdout_tx
    );
    
    node.run().await;

    Ok(())
}

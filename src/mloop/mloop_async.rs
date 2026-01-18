use crate::mloop::receive_init_then_send_init_ok;
use crate::node::multikafkalog::MultiKafkaLogNode;
use crate::payloads::KafkaLogOrKvPayload;
use crate::stdout_json::StdoutJson;
use crate::Message;
use tokio::io::{AsyncBufReadExt, BufReader};

pub async fn main_loop_async() -> anyhow::Result<()> {
    let node_id = receive_init_then_send_init_ok()?;

    let (stdout_tx, mut stdout_rx) =
        tokio::sync::mpsc::unbounded_channel::<Message<KafkaLogOrKvPayload>>();
    let (stdin_tx, stdin_rx) =
        tokio::sync::mpsc::unbounded_channel::<Message<KafkaLogOrKvPayload>>();

    tokio::spawn(async move {
        let async_stdin = tokio::io::stdin();
        let mut reader = BufReader::new(async_stdin).lines();
        let mut stdout = StdoutJson::new();

        loop {
            tokio::select! {
                Some(msg) = stdout_rx.recv() => {
                    stdout.write(&msg).expect("failed to write to stdout");
                },
                stdin_msg = reader.next_line() => {
                    match stdin_msg {
                        Ok(Some(msg)) => {
                            let msg = serde_json::from_str::<Message<KafkaLogOrKvPayload>>(&msg)
                                .expect("msg deserialization failed");
                            stdin_tx.send(msg).expect("failed to send msg to stdout channel");
                        },
                        Err(e) => {
                            eprintln!("stdin read error: {}", e);
                            break;
                        }
                        _ => {
                            eprintln!("error stdin! {:?}", stdin_msg);
                        }
                    }
                },
            }
        }
    });

    let mut node = MultiKafkaLogNode::new(node_id, 5, stdin_rx, stdout_tx);

    node.run().await;

    Ok(())
}

use rustorm::mloop::main_loop;
use rustorm::node::kafkalog::KafkaLogNode;
use rustorm::payloads::KafkaLogPayload;

fn main() -> anyhow::Result<()> {
    main_loop::<KafkaLogNode, KafkaLogPayload, ()>()
}

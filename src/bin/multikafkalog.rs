use rustorm::mloop::main_loop_async;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    main_loop_async().await
}

use tracing::level_filters::LevelFilter;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(LevelFilter::DEBUG)
        // TODO 发布环境去除
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    api::run().await;
}

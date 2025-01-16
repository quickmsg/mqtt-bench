use tokio::signal;
use tracing::{debug, info, level_filters::LevelFilter};
use tracing_subscriber::FmtSubscriber;

use clap::Parser;
use types::{BrokerUpdateReq, PublishCreateUpdateReq, Qos, SubscribeCreateUpdateReq};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = String::from("127.0.0.1"))]
    host: String,

    #[arg(long, default_value_t = 1883)]
    port: u16,

    #[arg(long)]
    client: usize,

    #[arg(long)]
    tps: Option<usize>,

    #[arg(long)]
    topic: String,

    #[arg(long)]
    size: Option<usize>,

    #[arg(long)]
    payload: Option<String>,

    #[arg(long)]
    qos: u8,

    #[arg(long)]
    ifaddr: Option<String>,

    #[arg(long, default_value_t = String::from("pub"))]
    mode: String,

    #[arg(long, default_value_t = 2)]
    connect_interval: u64,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        // .with_max_level(LevelFilter::INFO)
        .with_max_level(LevelFilter::DEBUG)
        // TODO 发布环境去除
        // .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let local_ips = match &args.ifaddr {
        Some(ifaddr) => Some(ifaddr.split(',').map(|s| s.to_string()).collect::<Vec<_>>()),
        None => None,
    };

    info!("{:?}", args);

    let hosts = vec![args.host.clone()];

    bench::update_broker(BrokerUpdateReq {
        hosts,
        username: None,
        password: None,
        client_id: None,
        connect_interval: args.connect_interval,
        statistics_interval: 1,
        local_ips,
    })
    .await
    .unwrap();

    bench::create_group(types::GroupCreateReq {
        name: "test".to_string(),
        client_id: Some("{uuid}".to_string()),
        protocol_version: types::ProtocolVersion::V311,
        protocol: types::Protocol::Mqtt,
        port: args.port,
        client_count: Some(args.client),
        ssl_conf: None,
        clients: None,
    })
    .await;

    let groups = bench::list_groups().await;
    let qos = match args.qos {
        0 => Qos::AtMostOnce,
        1 => Qos::AtLeastOnce,
        2 => Qos::ExactlyOnce,
        _ => panic!("invalid qos"),
    };

    if args.mode == "pub" {
        let tps = match args.tps {
            Some(tps) => tps,
            None => panic!("tps is required"),
        };
        bench::create_publish(
            groups.list[0].id.clone(),
            PublishCreateUpdateReq {
                name: "todo".to_string(),
                topic: args.topic,
                qos,
                retain: false,
                tps,
                payload: args.payload,
                size: args.size,
                v311: None,
                v50: None,
            },
        )
        .await
        .unwrap();
    } else if args.mode == "sub" {
        bench::create_subscribe(
            groups.list[0].id.clone(),
            SubscribeCreateUpdateReq {
                name: "todo".to_string(),
                topic: args.topic,
                qos,
                v311: None,
                v50: None,
            },
        )
        .await
        .unwrap();
    } else {
        panic!("invalid mode");
    }

    bench::start_group(groups.list[0].id.clone()).await;
    // debug!("{:?}", args);

    signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C signal");
}

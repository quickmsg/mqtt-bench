use std::fs::File;

use csv::ReaderBuilder;
use tokio::signal;
use tracing::{debug, level_filters::LevelFilter};
use tracing_subscriber::FmtSubscriber;

use clap::Parser;
use types::{BrokerUpdateReq, PublishCreateUpdateReq, Qos};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = String::from("127.0.0.1"))]
    host: String,

    #[arg(long, default_value_t = 1883)]
    port: u16,

    #[arg(long)]
    tps: usize,

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
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        // .with_max_level(LevelFilter::INFO)
        .with_max_level(LevelFilter::TRACE)
        // TODO 发布环境去除
        // .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let local_ips = match &args.ifaddr {
        Some(ifaddr) => Some(ifaddr.split(',').map(|s| s.to_string()).collect::<Vec<_>>()),
        None => None,
    };

    debug!("{:?}", args);

    let hosts = vec![args.host.clone()];

    bench::update_broker(BrokerUpdateReq {
        hosts,
        username: None,
        password: None,
        client_id: None,
        connect_interval: 1,
        statistics_interval: 1,
        local_ips,
    })
    .await
    .unwrap();

    let file = File::open("clients.txt").unwrap();
    let mut rdr = ReaderBuilder::new()
        .delimiter(b',')
        .has_headers(true)
        .from_reader(file);

    let mut client_confs = vec![];
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.

        let record = result.unwrap();
        debug!("{:?}", record);
        client_confs.push((
            record.get(1).unwrap().to_string(),
            record.get(2).unwrap().to_string(),
            record.get(3).unwrap().to_string(),
            record.get(4).unwrap().to_string(),
        ));
    }

    debug!("{:?}", client_confs);

    bench::create_group(types::GroupCreateReq {
        name: "test".to_string(),
        client_id: None,
        protocol_version: types::ProtocolVersion::V311,
        protocol: types::Protocol::Mqtt,
        port: 1883,
        client_count: None,
        ssl_conf: None,
        clients: Some(client_confs),
    })
    .await;

    let groups = bench::list_groups().await;
    let qos = match args.qos {
        0 => Qos::AtMostOnce,
        1 => Qos::AtLeastOnce,
        2 => Qos::ExactlyOnce,
        _ => panic!("invalid qos"),
    };
    bench::create_publish(
        groups.list[0].id.clone(),
        PublishCreateUpdateReq {
            name: "todo".to_string(),
            topic: args.topic,
            qos,
            retain: false,
            tps: args.tps,
            payload: args.payload,
            size: args.size,
            v311: None,
            v50: None,
            range: None,
        },
    )
    .await
    .unwrap();

    bench::start_group(groups.list[0].id.clone()).await;

    signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C signal");
}

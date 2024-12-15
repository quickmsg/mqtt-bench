use axum::{
    extract::Path,
    routing::{get, post, put},
    Json, Router,
};
use tokio::net::TcpListener;
use types::{BrokerInfo, GroupConf};

pub async fn run() {
    let app = Router::new()
        .route("/broker", get(read_broker).put(update_broker))
        .route("/group", post(create_group).get(list_groups))
        .route(
            "/group/:id",
            get(read_group).put(update_group).delete(delete_group),
        )
        .route("/group/:id/start", put(start_group))
        .route("/group/:id/stop", put(stop_group));

    let listener = TcpListener::bind(format!("0.0.0.0:{}", 5000))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[axum::debug_handler]
async fn read_broker() -> Json<BrokerInfo> {
    Json(bench::read_broker().await)
}

async fn update_broker(Json(broker_info): Json<BrokerInfo>) {
    bench::update_broker(broker_info).await;
}

async fn create_group(Json(group_conf): Json<GroupConf>) {
    bench::create_group(group_conf).await;
}

async fn list_groups() -> Json<Vec<GroupConf>> {
    Json(bench::list_groups().await)
}

async fn read_group() {}

async fn update_group() {}

async fn delete_group() {}

async fn start_group(Path(id): Path<String>) {
    bench::start_group(id).await;
}

async fn stop_group() {}

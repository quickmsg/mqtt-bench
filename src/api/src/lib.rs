use axum::{
    extract::Path,
    routing::{get, post, put},
    Json, Router,
};
use tokio::net::TcpListener;
use types::{
    BrokerUpdateReq, GroupCreateUpdateReq, ListGroupResp, PublishCreateUpdateReq, ReadGroupResp,
    SubscribeCreateUpdateReq,
};

pub async fn run() {
    let app = Router::new().nest(
        "/api",
        Router::new()
            .nest(
                "/broker",
                Router::new().route("/", get(read_broker).put(update_broker)),
            )
            .nest(
                "/group",
                Router::new()
                    .route("/", post(create_group).get(list_groups))
                    .nest(
                        "/:group_id",
                        Router::new()
                            .route("/", get(read_group).put(update_group).delete(delete_group))
                            .route("/start", put(start_group))
                            .route("/stop", put(stop_group))
                            .nest(
                                "/publish",
                                Router::new()
                                    .route("/", post(create_publish).get(list_publishes))
                                    .nest(
                                        "/:publish_id",
                                        Router::new().route(
                                            "/",
                                            get(read_publish)
                                                .put(update_publish)
                                                .delete(delete_publish),
                                        ),
                                    ),
                            )
                            .nest(
                                "/subscribe",
                                Router::new()
                                    .route("/", post(create_subscribe).get(list_subscribes))
                                    .nest(
                                        "/:subscribe_id",
                                        Router::new().route(
                                            "/",
                                            get(read_subscribe)
                                                .put(update_subscribe)
                                                .delete(delete_subscribe),
                                        ),
                                    ),
                            ),
                    ),
            ),
    );

    let listener = TcpListener::bind(format!("0.0.0.0:{}", 5000))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[axum::debug_handler]
async fn read_broker() -> Json<BrokerUpdateReq> {
    Json(bench::read_broker().await)
}

async fn update_broker(Json(req): Json<BrokerUpdateReq>) {
    bench::update_broker(req).await;
}

async fn create_group(Json(req): Json<GroupCreateUpdateReq>) {
    bench::create_group(req).await;
}

async fn list_groups() -> Json<ListGroupResp> {
    Json(bench::list_groups().await)
}

async fn read_group(Path(group_id): Path<String>) -> Json<ReadGroupResp> {
    Json(bench::read_group(group_id).await)
}

async fn update_group() {}

async fn delete_group() {}

async fn start_group(Path(group_id): Path<String>) {
    bench::start_group(group_id).await;
}

async fn stop_group(Path(group_id): Path<String>) {
    bench::stop_group(group_id).await;
}

async fn create_publish(Path(group_id): Path<String>, Json(req): Json<PublishCreateUpdateReq>) {
    println!("{}", group_id);
    bench::create_publish(group_id, req).await;
}

async fn list_publishes(Path(group_id): Path<String>) {}

async fn read_publish(Path((group_id, publish_id)): Path<(String, String)>) {}

async fn update_publish(
    Path((group_id, publish_id)): Path<(String, String)>,
    Json(req): Json<PublishCreateUpdateReq>,
) {
}

async fn delete_publish(Path((group_id, publish_id)): Path<(String, String)>) {}

async fn create_subscribe(Path(group_id): Path<String>, Json(req): Json<SubscribeCreateUpdateReq>) {
}

async fn list_subscribes(Path(group_id): Path<String>) {}

async fn read_subscribe(Path((group_id, subscribe_id)): Path<(String, String)>) {}

async fn update_subscribe(
    Path((group_id, subscribe_id)): Path<(String, String)>,
    Json(req): Json<SubscribeCreateUpdateReq>,
) {
}

async fn delete_subscribe(Path((group_id, subscribe_id)): Path<(String, String)>) {
    todo!()
}

use std::result;

use anyhow::Error;
use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use types::{
    BrokerUpdateReq, ClientsListResp, ClientsQueryParams, GroupCreateReq, GroupListResp,
    GroupUpdateReq, ListPublishResp, ListSubscribeResp, MetricsListResp, MetricsQueryParams,
    PublishCreateUpdateReq, ReadGroupResp, SubscribeCreateUpdateReq,
};

type AppResult<T, E = AppError> = result::Result<T, E>;

struct AppError {
    data: String,
}

impl From<Error> for AppError {
    fn from(value: Error) -> Self {
        Self {
            data: value.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, self.data).into_response()
    }
}

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
                            .route("/metrics", get(read_metrics))
                            .route("/start", put(start_group))
                            .route("/stop", put(stop_group))
                            .route("/clients", get(list_clients))
                            .nest(
                                "/publish",
                                Router::new()
                                    .route("/", post(create_publish).get(list_publishes))
                                    .nest(
                                        "/:publish_id",
                                        Router::new()
                                            .route("/", put(update_publish).delete(delete_publish)),
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
                                            put(update_subscribe).delete(delete_subscribe),
                                        ),
                                    ),
                            ),
                    ),
            )
            .layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any),
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

async fn update_broker(Json(req): Json<BrokerUpdateReq>) -> AppResult<()> {
    bench::update_broker(req).await?;
    Ok(())
}

async fn create_group(Json(req): Json<GroupCreateReq>) {
    bench::create_group(req).await;
}

async fn list_groups() -> Json<GroupListResp> {
    Json(bench::list_groups().await)
}

async fn read_group(Path(group_id): Path<String>) -> Json<ReadGroupResp> {
    Json(bench::read_group(group_id).await)
}

async fn update_group(
    Path(group_id): Path<String>,
    Json(req): Json<GroupUpdateReq>,
) -> AppResult<()> {
    bench::update_group(group_id, req).await?;
    Ok(())
}

async fn delete_group(Path(group_id): Path<String>) {
    bench::delete_group(group_id).await;
}

async fn read_metrics(
    Path(group_id): Path<String>,
    Query(query): Query<MetricsQueryParams>,
) -> Json<MetricsListResp> {
    Json(bench::read_metrics(group_id, query).await)
}

async fn start_group(Path(group_id): Path<String>) {
    bench::start_group(group_id).await;
}

async fn stop_group(Path(group_id): Path<String>) {
    bench::stop_group(group_id).await;
}

async fn create_publish(
    Path(group_id): Path<String>,
    Json(req): Json<PublishCreateUpdateReq>,
) -> AppResult<()> {
    bench::create_publish(group_id, req).await?;
    Ok(())
}

async fn list_publishes(Path(group_id): Path<String>) -> Json<ListPublishResp> {
    Json(bench::list_publishes(group_id).await.unwrap())
}

async fn update_publish(
    Path((group_id, publish_id)): Path<(String, String)>,
    Json(req): Json<PublishCreateUpdateReq>,
) -> AppResult<()> {
    bench::update_publish(group_id, publish_id, req).await?;
    Ok(())
}

async fn delete_publish(Path((group_id, publish_id)): Path<(String, String)>) -> AppResult<()> {
    bench::delete_publish(group_id, publish_id).await?;
    Ok(())
}

async fn create_subscribe(
    Path(group_id): Path<String>,
    Json(req): Json<SubscribeCreateUpdateReq>,
) -> AppResult<()> {
    bench::create_subscribe(group_id, req).await?;
    Ok(())
}

async fn list_subscribes(Path(group_id): Path<String>) -> Json<ListSubscribeResp> {
    Json(bench::list_subscribes(group_id).await)
}

async fn update_subscribe(
    Path((group_id, subscribe_id)): Path<(String, String)>,
    Json(req): Json<SubscribeCreateUpdateReq>,
) -> AppResult<()> {
    bench::update_subscribe(group_id, subscribe_id, req).await?;
    Ok(())
}

async fn delete_subscribe(Path((group_id, subscribe_id)): Path<(String, String)>) -> AppResult<()> {
    bench::delete_subscribe(group_id, subscribe_id).await?;
    Ok(())
}

async fn list_clients(
    Path(group_id): Path<String>,
    Query(query): Query<ClientsQueryParams>,
) -> Json<ClientsListResp> {
    Json(bench::list_clients(group_id, query).await)
}

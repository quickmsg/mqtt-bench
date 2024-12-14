use std::sync::Arc;

use axum::{
    extract::State,
    routing::{get, post, put},
    Json, Router,
};
use bench::ClientManager;
use tokio::{net::TcpListener, sync::RwLock};
use types::{ClientStatus, TaskCreateReq};

struct AppState {
    pub client_manager: RwLock<Option<ClientManager>>,
}
pub async fn run() {
    let shared_state = Arc::new(AppState {
        client_manager: RwLock::new(None),
    });

    let app = Router::new()
        .route("/", post(create))
        .route("/start", put(start))
        .route("/status", get(status))
        .with_state(shared_state.clone());

    let listener = TcpListener::bind(format!("0.0.0.0:{}", 5000))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn create(State(state): State<Arc<AppState>>, Json(req): Json<TaskCreateReq>) {
    let client_manager = ClientManager::new(req.mqtt_server_info, req.connect_bench_info);
    state.client_manager.write().await.replace(client_manager);
}

async fn update(State(state): State<Arc<AppState>>) {}

async fn start(State(state): State<Arc<AppState>>) {
    let mut client_manager_guard = state.client_manager.write().await;
    client_manager_guard.as_mut().unwrap().start().await;
}

async fn status(State(state): State<Arc<AppState>>) -> Json<Vec<ClientStatus>> {
    let client_manager_guard = state.client_manager.read().await;
    let status = client_manager_guard.as_ref().unwrap().status().await;
    Json(status)
}

async fn stop(State(state): State<Arc<AppState>>) {}

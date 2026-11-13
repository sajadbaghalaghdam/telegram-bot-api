//! Shared helpers for tests that drive the whole HTTP stack against a recording mock upstream.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{header, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use clap::Parser;

use crate::client::{ClientManager, TelegramEngine};
use crate::config::ServerConfig;
use crate::server::{create_router, AppState};

pub const TOKEN: &str = "123456:AAH-abcdefghijklmnopqrstuvwxyz012345";

/// One request received by the mock upstream.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    /// Path and query exactly as sent.
    pub target: String,
    pub content_type: Option<String>,
    pub body: Bytes,
}

/// Canned upstream answer.
pub struct Reply {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(&'static str, String)>,
}

impl Reply {
    pub fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.as_bytes().to_vec(),
            headers: vec![("content-type", "application/json".to_string())],
        }
    }

    pub fn bytes(status: u16, body: Vec<u8>) -> Self {
        Self { status, body, headers: Vec::new() }
    }
}

type Responder = Arc<dyn Fn(&Recorded) -> Reply + Send + Sync>;
type Log = Arc<Mutex<Vec<Recorded>>>;

/// HTTP server on an ephemeral port that records every request and answers through `responder`.
pub struct MockUpstream {
    pub url: String,
    log: Log,
}

impl MockUpstream {
    pub async fn start(responder: impl Fn(&Recorded) -> Reply + Send + Sync + 'static) -> Self {
        let log: Log = Arc::default();
        let state: (Responder, Log) = (Arc::new(responder), Arc::clone(&log));
        let app = axum::Router::new().fallback(handle).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url, log }
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.log.lock().unwrap().clone()
    }
}

async fn handle(State((responder, log)): State<(Responder, Log)>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, usize::MAX).await.unwrap_or_default();
    let rec = Recorded {
        method: parts.method.to_string(),
        target: parts.uri.path_and_query().map(|p| p.to_string()).unwrap_or_default(),
        content_type: parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        body,
    };
    let reply = responder(&rec);
    log.lock().unwrap().push(rec);
    let mut resp = (StatusCode::from_u16(reply.status).unwrap(), reply.body).into_response();
    for (k, v) in reply.headers {
        resp.headers_mut()
            .insert(HeaderName::from_static(k), HeaderValue::from_str(&v).unwrap());
    }
    resp
}

/// A running application (router + engine) wired to a mock upstream.
pub struct TestApp {
    pub base: String,
    pub engine: Arc<TelegramEngine>,
    pub http: reqwest::Client,
    pub dir: PathBuf,
}

impl TestApp {
    pub async fn start(name: &str, local: bool, upstream: &MockUpstream) -> Self {
        Self::start_at(name, local, &upstream.url).await
    }

    /// Like [`start`](Self::start) for an upstream that is not a `MockUpstream`.
    pub async fn start_at(name: &str, local: bool, upstream_url: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("tgapp_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let storage = crate::storage::StorageEngine::new(&dir).unwrap();
        let mut engine = TelegramEngine::new(local, dir.clone(), dir.clone(), storage);
        engine.upstream_base_url = upstream_url.to_string();
        let engine = Arc::new(engine);
        let config = ServerConfig::parse_from(if local { vec!["bot", "--local"] } else { vec!["bot"] });
        let state = AppState {
            config: Arc::new(config),
            engine: Arc::clone(&engine),
            client_manager: Arc::new(ClientManager::new(Arc::clone(&engine))),
            stats: Arc::new(crate::stats::ServerStats::new()),
            watchdog: crate::watchdog::Watchdog::new(60),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = create_router(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            base,
            engine,
            http: reqwest::Client::new(),
            dir: std::fs::canonicalize(&dir).unwrap(),
        }
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Status, content type, Retry-After and body of a response.
pub struct Got {
    pub status: u16,
    pub content_type: Option<String>,
    pub retry_after: Option<String>,
    pub body: Vec<u8>,
}

impl Got {
    pub async fn from(resp: reqwest::Response) -> Self {
        let h = resp.headers();
        let get = |n: &str| h.get(n).and_then(|v| v.to_str().ok()).map(str::to_string);
        let (content_type, retry_after) = (get("content-type"), get("retry-after"));
        let status = resp.status().as_u16();
        Self { status, content_type, retry_after, body: resp.bytes().await.unwrap().to_vec() }
    }

    pub fn text(&self) -> &str {
        std::str::from_utf8(&self.body).unwrap()
    }
}

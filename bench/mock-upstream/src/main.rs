//! Minimal mock of the cloud Bot API (api.telegram.org) plus a webhook receiver, for benchmarks.
//!
//! Usage: mock-upstream [--port 18081] [--threads 2]      (env FILE_BYTES, default 65536)
//!
//! Bot API (any token is accepted; routes are `/bot<token>/<method>`):
//!   getMe                 fixed bot profile
//!   sendMessage           fixed Message (request body is consumed and dropped)
//!   getUpdates            stateful: serves seeded updates by offset/limit; when nothing is
//!                         available it long-polls up to `timeout` seconds (capped at 25)
//!                         and then returns an empty list
//!   getFile               fixed File {file_path:"documents/file_1.bin"}
//!   anything else         {"ok":true,"result":true}   (setWebhook, deleteWebhook, ...)
//!   GET /file/bot<token>/<path>   FILE_BYTES bytes of 'x'
//! Webhook receiver:
//!   POST /hook            counts the delivery, answers 200 with an empty body
//! Control:
//!   GET /__mock/seed?n=N&chats=C   (re)seed N updates (ids 1..N) spread over C chats
//!   GET /__mock/stats              JSON counters
//!   GET /__mock/reset              zero the counters
use axum::{
    Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering::Relaxed},
};
use std::time::Duration;
use tokio::sync::Notify;

const JSON: [(header::HeaderName, &str); 1] = [(header::CONTENT_TYPE, "application/json")];
const GET_ME: &str = r#"{"ok":true,"result":{"id":123456789,"is_bot":true,"first_name":"BenchBot","username":"bench_bot","can_join_groups":true,"can_read_all_group_messages":false,"supports_inline_queries":false}}"#;
const SEND_MESSAGE: &str = r#"{"ok":true,"result":{"message_id":42,"from":{"id":123456789,"is_bot":true,"first_name":"BenchBot","username":"bench_bot"},"chat":{"id":1001,"first_name":"Tester","username":"tester","type":"private"},"date":1700000000,"text":"hello"}}"#;
const GET_FILE: &str = r#"{"ok":true,"result":{"file_id":"BQACAgIAAxkBAAIBQ2","file_unique_id":"AgADBench","file_size":65536,"file_path":"documents/file_1.bin"}}"#;
const OK_TRUE: &str = r#"{"ok":true,"result":true}"#;
const EMPTY: &str = r#"{"ok":true,"result":[]}"#;

#[derive(Default)]
struct Mock {
    updates: Mutex<Vec<Arc<str>>>,
    seeded: Notify,
    hook: AtomicU64,
    hook_bytes: AtomicU64,
    get_updates: AtomicU64,
    send_message: AtomicU64,
    get_me: AtomicU64,
    other: AtomicU64,
    file_bytes: usize,
}

type S = State<Arc<Mock>>;

fn json(body: &'static str) -> Response {
    (StatusCode::OK, JSON, body).into_response()
}

async fn bot(State(m): S, Path((_token, method)): Path<(String, String)>, body: Bytes) -> Response {
    match method.as_str() {
        "getMe" => {
            m.get_me.fetch_add(1, Relaxed);
            json(GET_ME)
        }
        "sendMessage" => {
            m.send_message.fetch_add(1, Relaxed);
            json(SEND_MESSAGE)
        }
        "getFile" => json(GET_FILE),
        "getUpdates" => get_updates(m, body).await,
        _ => {
            m.other.fetch_add(1, Relaxed);
            json(OK_TRUE)
        }
    }
}

async fn get_updates(m: Arc<Mock>, body: Bytes) -> Response {
    m.get_updates.fetch_add(1, Relaxed);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let offset = v["offset"].as_i64().unwrap_or(0).max(1) as usize;
    let limit = v["limit"].as_u64().unwrap_or(100).clamp(1, 100) as usize;
    let timeout = v["timeout"].as_u64().unwrap_or(0).min(25);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout);
    loop {
        let notified = m.seeded.notified();
        let page: Vec<Arc<str>> = {
            let g = m.updates.lock().unwrap();
            g.iter().skip(offset - 1).take(limit).cloned().collect()
        };
        if !page.is_empty() {
            let mut out = String::from(r#"{"ok":true,"result":["#);
            for (i, u) in page.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(u);
            }
            out.push_str("]}");
            return (StatusCode::OK, JSON, out).into_response();
        }
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            return json(EMPTY);
        }
    }
}

async fn file(State(m): S) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        vec![b'x'; m.file_bytes],
    )
        .into_response()
}

async fn hook(State(m): S, body: Bytes) -> StatusCode {
    m.hook.fetch_add(1, Relaxed);
    m.hook_bytes.fetch_add(body.len() as u64, Relaxed);
    StatusCode::OK
}

async fn seed(State(m): S, Query(q): Query<HashMap<String, String>>) -> &'static str {
    let n: usize = q.get("n").and_then(|v| v.parse().ok()).unwrap_or(1000);
    let chats: usize = q.get("chats").and_then(|v| v.parse().ok()).unwrap_or(100).max(1);
    let v: Vec<Arc<str>> = (1..=n)
        .map(|id| {
            let chat = 1000 + (id % chats);
            Arc::from(format!(
                r#"{{"update_id":{id},"message":{{"message_id":{id},"from":{{"id":{chat},"is_bot":false,"first_name":"T"}},"chat":{{"id":{chat},"first_name":"T","type":"private"}},"date":1700000000,"text":"benchmark update {id}"}}}}"#
            ))
        })
        .collect();
    *m.updates.lock().unwrap() = v;
    m.seeded.notify_waiters();
    "seeded\n"
}

async fn stats(State(m): S) -> Response {
    let s = format!(
        r#"{{"hook":{},"hook_bytes":{},"get_updates":{},"send_message":{},"get_me":{},"other":{},"seeded":{}}}"#,
        m.hook.load(Relaxed),
        m.hook_bytes.load(Relaxed),
        m.get_updates.load(Relaxed),
        m.send_message.load(Relaxed),
        m.get_me.load(Relaxed),
        m.other.load(Relaxed),
        m.updates.lock().unwrap().len()
    );
    (StatusCode::OK, JSON, s).into_response()
}

async fn reset(State(m): S) -> &'static str {
    for c in [&m.hook, &m.hook_bytes, &m.get_updates, &m.send_message, &m.get_me, &m.other] {
        c.store(0, Relaxed);
    }
    "reset\n"
}

fn arg(name: &str, default: usize) -> usize {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == name)
        .and_then(|i| a.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let port = arg("--port", 18081);
    let threads = arg("--threads", 2);
    let file_bytes = std::env::var("FILE_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(65536);
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build().unwrap();
    rt.block_on(async move {
        let m = Arc::new(Mock { file_bytes, ..Default::default() });
        let app = Router::new()
            .route("/hook", post(hook))
            .route("/__mock/seed", get(seed))
            .route("/__mock/stats", get(stats))
            .route("/__mock/reset", get(reset))
            .route("/file/{token}/{*path}", get(file))
            .route("/{token}/{method}", any(bot))
            .with_state(m);
        let l = tokio::net::TcpListener::bind(("127.0.0.1", port as u16)).await.unwrap();
        eprintln!("mock-upstream listening on 127.0.0.1:{port} ({threads} threads)");
        axum::serve(l, app).await.unwrap();
    });
}

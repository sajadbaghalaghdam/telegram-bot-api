//! End-to-end tests of the webhook worker against mock upstream / webhook servers.
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde_json::Value;

use super::delivery::MAX_REPLY_BYTES;
use super::WorkerConfig;
use crate::client::engine::TelegramEngine;
use crate::error::TelegramError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

type Log = Arc<Mutex<Vec<String>>>;

async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<(String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = sock.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..pos]).to_string();
            let cl = head
                .lines()
                .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap_or(0)))
                .unwrap_or(0);
            while buf.len() < pos + 4 + cl {
                let n = sock.read(&mut tmp).await.ok()?;
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            return Some((head, buf[pos + 4..].to_vec()));
        }
    }
}

async fn respond(sock: &mut tokio::net::TcpStream, status: &str, body: &str) {
    let r = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = sock.write_all(r.as_bytes()).await;
    let _ = sock.shutdown().await;
}

fn test_engine(name: &str, up_port: u16) -> (Arc<TelegramEngine>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("{name}_{}", std::process::id()));
    let storage = crate::storage::StorageEngine::new(&dir).unwrap();
    let mut engine = TelegramEngine::new(true, dir.clone(), dir.clone(), storage);
    engine.upstream_base_url = format!("http://127.0.0.1:{up_port}");
    (Arc::new(engine), dir)
}

fn cfg(token: &str, url: String) -> WorkerConfig {
    WorkerConfig {
        token: token.to_string(),
        url,
        secret_token: None,
        allowed_updates: None,
        drop_pending_updates: false,
        max_connections: 5,
        ip_address: None,
    }
}

#[tokio::test]
async fn offset_advances_only_after_webhook_2xx() {
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    // Mock webhook: 500 first, then 200.
    let wh = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wh_port = wh.local_addr().unwrap().port();
    let wlog = log.clone();
    tokio::spawn(async move {
        let mut n = 0;
        loop {
            let (mut s, _) = wh.accept().await.unwrap();
            let Some((_, body)) = read_request(&mut s).await else { continue };
            n += 1;
            wlog.lock().unwrap().push(format!("wh:{}", String::from_utf8_lossy(&body)));
            if n == 1 {
                respond(&mut s, "500 Internal Server Error", "boom").await;
            } else {
                respond(&mut s, "200 OK", "").await;
            }
        }
    });

    // Mock upstream: update 7 for offset 0; empty afterwards.
    let up = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_port = up.local_addr().unwrap().port();
    let ulog = log.clone();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = up.accept().await.unwrap();
            let ulog = ulog.clone();
            tokio::spawn(async move {
                let Some((_, body)) = read_request(&mut s).await else { return };
                let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let off = v.get("offset").and_then(|o| o.as_i64()).unwrap_or(-99);
                ulog.lock().unwrap().push(format!("poll:{off}"));
                if off == 0 {
                    respond(&mut s, "200 OK", r#"{"ok":true,"result":[{"update_id":7,"message":{"chat":{"id":1},"text":"hi"}}]}"#).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    respond(&mut s, "200 OK", r#"{"ok":true,"result":[]}"#).await;
                }
            });
        }
    });

    let (engine, dir) = test_engine("wh_it", up_port);
    let token = "123456:ABCDEF".to_string();
    engine.start_webhook_worker(cfg(&token, format!("http://127.0.0.1:{wh_port}/hook")));

    let mut seen = Vec::new();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        seen = log.lock().unwrap().clone();
        if seen.iter().any(|e| e == "poll:8") {
            break;
        }
    }
    // The offset never goes past the update before the webhook answered 2xx: the polls in
    // between (re-fetching the unconfirmed update) stay at <= 7, and poll:8 follows the 200.
    assert_eq!(seen[0], "poll:0");
    let whs: Vec<usize> = (0..seen.len()).filter(|&i| seen[i].starts_with("wh:")).collect();
    assert_eq!(whs.len(), 2, "500 then 200 for the same update: {seen:?}");
    assert_eq!(seen[whs[0]], seen[whs[1]], "same update twice: {seen:?}");
    let p8 = seen.iter().position(|e| e == "poll:8").unwrap();
    assert!(p8 > whs[1], "offset must advance only after the 200: {seen:?}");
    assert!(seen[..p8].iter().filter_map(|e| e.strip_prefix("poll:")).all(|o| o.parse::<i64>().unwrap() <= 7), "{seen:?}");

    // getWebhookInfo-visible state: error recorded, nothing pending.
    let st = engine.webhook_states.get(&token).unwrap().clone();
    let (pending, date, msg) = st.snapshot();
    assert_eq!(pending, 0);
    assert!(date.is_some());
    assert!(msg.unwrap().contains("500"));
    engine.stop_webhook_worker(&token);
    assert!(!engine.webhook_workers.contains_key(&token));
    let _ = std::fs::remove_dir_all(&dir);
}

type Recorded = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;

/// Upstream mock: records (request head, path, body) and answers via `f(path) -> (status, body)`.
async fn mock_upstream(rec: Recorded, f: impl Fn(&str) -> (&'static str, String) + Send + Sync + 'static) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let f = Arc::new(f);
    tokio::spawn(async move {
        loop {
            let (mut s, _) = l.accept().await.unwrap();
            let (rec, f) = (rec.clone(), f.clone());
            tokio::spawn(async move {
                let Some((head, body)) = read_request(&mut s).await else { return };
                let path = head.lines().next().unwrap_or("").split(' ').nth(1).unwrap_or("").to_string();
                rec.lock().unwrap().push((head, path.clone(), body));
                if path.contains("/getUpdates") {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                let (st, b) = f(&path);
                respond(&mut s, st, &b).await;
            });
        }
    });
    port
}

fn ok_upstream(path: &str) -> (&'static str, String) {
    if path.contains("/getUpdates") {
        ("200 OK", r#"{"ok":true,"result":[]}"#.to_string())
    } else {
        ("200 OK", r#"{"ok":true,"result":true}"#.to_string())
    }
}

#[tokio::test]
async fn cloud_set_webhook_forwards_original_request_for_every_body_type() {
    let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = mock_upstream(rec.clone(), ok_upstream).await;
    let (engine, dir) = test_engine("wh_cloud", port);

    let multipart = "--B1\r\nContent-Disposition: form-data; name=\"url\"\r\n\r\nhttps://example.com/h\r\n--B1\r\nContent-Disposition: form-data; name=\"certificate\"; filename=\"c.pem\"\r\n\r\nPEMDATA\r\n--B1--\r\n";
    let cases: Vec<(&str, Option<&str>, Vec<u8>)> = vec![
        ("111:a", Some("application/json"), br#"{"url":"https://example.com/h","max_connections":3}"#.to_vec()),
        ("222:b", Some("application/x-www-form-urlencoded"), b"url=https%3A%2F%2Fexample.com%2Fh&max_connections=3".to_vec()),
        ("333:c", Some("multipart/form-data; boundary=B1"), multipart.as_bytes().to_vec()),
    ];
    for (token, ct, body) in &cases {
        let out = engine
            .universal_call(token, "setWebhook", &HashMap::new(), Some("x=1"), *ct, Bytes::from(body.clone()))
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&out).contains("\"ok\":true"));
    }
    let seen = rec.lock().unwrap().clone();
    assert_eq!(seen.len(), 3, "{seen:?}");
    for ((token, ct, body), (head, path, got)) in cases.iter().zip(seen.iter()) {
        assert_eq!(path, &format!("/bot{token}/setWebhook?x=1"));
        assert!(head.to_ascii_lowercase().contains(&format!("content-type: {}", ct.unwrap().to_ascii_lowercase())), "{head}");
        assert_eq!(got, body, "body must be forwarded byte for byte");
    }
    // the certificate part survives (it is only dropped by our own arg parser)
    assert!(String::from_utf8_lossy(&seen[2].2).contains("PEMDATA"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn local_set_webhook_accepts_form_and_multipart_bodies() {
    let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = mock_upstream(rec.clone(), ok_upstream).await;
    let (engine, dir) = test_engine("wh_local_forms", port);
    let multipart = "--B1\r\nContent-Disposition: form-data; name=\"url\"\r\n\r\nhttp://127.0.0.1:9/h\r\n--B1\r\nContent-Disposition: form-data; name=\"max_connections\"\r\n\r\n4\r\n--B1--\r\n";
    let cases: Vec<(&str, &str, Vec<u8>)> = vec![
        ("444:a", "application/x-www-form-urlencoded", b"url=http%3A%2F%2F127.0.0.1%3A9%2Fh&max_connections=4".to_vec()),
        ("555:b", "multipart/form-data; boundary=B1", multipart.as_bytes().to_vec()),
    ];
    for (token, ct, body) in cases {
        engine
            .universal_call(token, "setWebhook", &HashMap::new(), None, Some(ct), Bytes::from(body))
            .await
            .unwrap();
        let rec_wh = engine.storage.get_webhook(token).await.expect("stored");
        assert_eq!(rec_wh.url, "http://127.0.0.1:9/h");
        assert_eq!(rec_wh.max_connections, 4);
        assert!(engine.webhook_workers.contains_key(token));
        engine.stop_webhook_worker(token);
    }
    assert!(rec.lock().unwrap().iter().any(|(_, p, _)| p.contains("/bot444:a/deleteWebhook")));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unparsable_body_never_touches_upstream_or_local_state() {
    let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = mock_upstream(rec.clone(), ok_upstream).await;
    let (engine, dir) = test_engine("wh_badbody", port);
    engine.storage.save_webhook("666:a", "http://127.0.0.1:9/h", None, 40, None, None).await;
    let r = engine
        .universal_call("666:a", "setWebhook", &HashMap::new(), None, Some("application/json"), Bytes::from_static(b"{oops"))
        .await;
    assert!(matches!(r, Err(TelegramError::BadRequest(_))), "{r:?}");
    assert!(engine.storage.get_webhook("666:a").await.is_some());
    assert!(rec.lock().unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn local_set_webhook_fails_when_upstream_delete_fails() {
    let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = mock_upstream(rec.clone(), |_| {
        ("401 Unauthorized", r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#.to_string())
    })
    .await;
    let (engine, dir) = test_engine("wh_delfail", port);
    let r = engine
        .universal_call(
            "777:a",
            "setWebhook",
            &HashMap::new(),
            None,
            Some("application/json"),
            Bytes::from_static(br#"{"url":"http://127.0.0.1:9/h"}"#),
        )
        .await;
    assert!(matches!(r, Err(TelegramError::ApiError { code: 401, .. })), "{r:?}");
    assert!(engine.storage.get_webhook("777:a").await.is_none());
    assert!(!engine.webhook_workers.contains_key("777:a"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn invalid_ip_and_secret_rules() {
    let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = mock_upstream(rec.clone(), ok_upstream).await;
    let (engine, dir) = test_engine("wh_ip", port);
    let call = |token: &'static str, body: &'static [u8]| {
        let engine = engine.clone();
        async move {
            engine
                .universal_call(token, "setWebhook", &HashMap::new(), None, Some("application/json"), Bytes::from_static(body))
                .await
        }
    };
    let r = call("881:a", br#"{"url":"https://example.com/h","ip_address":"not-an-ip"}"#).await;
    assert!(matches!(r, Err(TelegramError::BadRequest(_))), "{r:?}");
    // bad secret with a url is rejected, but ignored when the webhook is being deleted
    let r = call("882:a", br#"{"url":"https://example.com/h","secret_token":"bad token"}"#).await;
    assert!(matches!(r, Err(TelegramError::BadRequest(_))), "{r:?}");
    let r = call("883:a", br#"{"url":"","secret_token":"bad token"}"#).await;
    assert!(r.is_ok(), "{r:?}");
    // scheme-less url is accepted (https:// assumed) and forwarded
    let r = call("884:a", br#"{"url":"example.com/h"}"#).await;
    assert!(r.is_ok(), "{r:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Upstream that serves a fixed update list like the real getUpdates: everything with
/// `update_id >= offset`, at most `limit`, instantly when non-empty, else after a short wait.
async fn serving_upstream(log: Log, updates: Vec<Value>) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = l.accept().await.unwrap();
            let (log, updates) = (log.clone(), updates.clone());
            tokio::spawn(async move {
                let Some((_, body)) = read_request(&mut s).await else { return };
                let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                let off = v["offset"].as_i64().unwrap_or(0);
                let limit = v["limit"].as_u64().unwrap_or(100) as usize;
                log.lock().unwrap().push(format!("poll:{off}"));
                let res: Vec<&Value> = updates.iter().filter(|u| u["update_id"].as_i64().unwrap() >= off).take(limit).collect();
                if res.is_empty() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                respond(&mut s, "200 OK", &serde_json::json!({"ok": true, "result": res}).to_string()).await;
            });
        }
    });
    port
}

async fn wait_for(log: &Log, what: &str, pred: impl Fn(&[String]) -> bool) -> Vec<String> {
    for _ in 0..200 {
        let seen = log.lock().unwrap().clone();
        if pred(&seen) {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timeout waiting for {what}: {:?}", log.lock().unwrap());
}

#[tokio::test]
async fn poisoned_chat_does_not_block_other_chats() {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    // Webhook: chat 1 fails twice (500) and then recovers; everything else is 200.
    let wh = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wh_port = wh.local_addr().unwrap().port();
    let wlog = log.clone();
    tokio::spawn(async move {
        let mut chat1_calls = 0;
        loop {
            let (mut s, _) = wh.accept().await.unwrap();
            let Some((_, body)) = read_request(&mut s).await else { continue };
            let v: Value = serde_json::from_slice(&body).unwrap();
            let id = v["update_id"].as_i64().unwrap();
            let chat = v["message"]["chat"]["id"].as_i64().unwrap();
            if chat == 1 {
                chat1_calls += 1;
                if chat1_calls <= 2 {
                    wlog.lock().unwrap().push(format!("wh:{id}:500"));
                    respond(&mut s, "500 Internal Server Error", "no").await;
                    continue;
                }
            }
            wlog.lock().unwrap().push(format!("wh:{id}:200"));
            respond(&mut s, "200 OK", "").await;
        }
    });
    let msg = |id: i64, chat: i64| serde_json::json!({"update_id": id, "message": {"chat": {"id": chat}, "text": "x"}});
    let up_port = serving_upstream(log.clone(), vec![msg(1, 1), msg(2, 2), msg(3, 2), msg(4, 3), msg(5, 2)]).await;

    let (engine, dir) = test_engine("wh_poison", up_port);
    let token = "123456:POISON".to_string();
    engine.start_webhook_worker(cfg(&token, format!("http://127.0.0.1:{wh_port}/hook")));

    // Chats 2 and 3 are fully delivered while chat 1 is still failing.
    let seen = wait_for(&log, "other chats delivered", |l| {
        ["wh:4:200", "wh:5:200"].iter().all(|w| l.iter().any(|e| e == w))
    })
    .await;
    assert!(!seen.iter().any(|e| e == "wh:1:200"), "chat 1 must still be failing: {seen:?}");
    // ordering inside chat 2: 2, 3, 5
    let order: Vec<&str> = seen
        .iter()
        .filter(|e| e.starts_with("wh:2:") || e.starts_with("wh:3:") || e.starts_with("wh:5:"))
        .map(|e| e.as_str())
        .collect();
    assert_eq!(order, ["wh:2:200", "wh:3:200", "wh:5:200"], "{seen:?}");
    // the watermark holds at the stuck update 1: nothing beyond it was confirmed upstream
    assert!(
        seen.iter().filter_map(|e| e.strip_prefix("poll:")).all(|o| o.parse::<i64>().unwrap() <= 1),
        "offset advanced past an undelivered update: {seen:?}"
    );

    // After the retry (2 s backoff) succeeded, the watermark jumps past everything.
    let seen = wait_for(&log, "offset 6", |l| l.iter().any(|e| e == "poll:6")).await;
    let ok1 = seen.iter().position(|e| e == "wh:1:200").expect("chat 1 recovered");
    let p6 = seen.iter().position(|e| e == "poll:6").unwrap();
    assert!(ok1 < p6);
    // re-sent unconfirmed updates were not delivered twice
    for id in 2..=5 {
        assert_eq!(seen.iter().filter(|e| **e == format!("wh:{id}:200")).count(), 1, "update {id} delivered once: {seen:?}");
    }
    let (pending, _, _) = engine.webhook_states.get(&token).unwrap().snapshot();
    assert_eq!(pending, 0);
    engine.stop_webhook_worker(&token);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Webhook mock answering 200 with `reply` for every request.
async fn reply_webhook(reply: Vec<u8>) -> u16 {
    let wh = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = wh.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = wh.accept().await.unwrap();
            let reply = reply.clone();
            tokio::spawn(async move {
                let _ = read_request(&mut s).await;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.len()
                );
                let _ = s.write_all(head.as_bytes()).await;
                let _ = s.write_all(&reply).await;
                let _ = s.shutdown().await;
            });
        }
    });
    port
}

/// Delivers one update to a webhook answering with `reply`; returns the upstream paths seen.
async fn run_reply_case(name: &str, reply: Vec<u8>) -> Vec<String> {
    let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
    let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let up = mock_upstream(rec.clone(), move |path| {
        if path.contains("/getUpdates") && first.swap(false, Ordering::SeqCst) {
            return ("200 OK", r#"{"ok":true,"result":[{"update_id":1,"message":{"chat":{"id":1}}}]}"#.to_string());
        }
        ok_upstream(path)
    })
    .await;
    let wh_port = reply_webhook(reply).await;
    let (engine, dir) = test_engine(name, up);
    let token = "123456:REPLY";
    engine.start_webhook_worker(cfg(token, format!("http://127.0.0.1:{wh_port}/hook")));
    // update 1 delivered => a poll with offset 2 follows
    for _ in 0..100 {
        if rec.lock().unwrap().iter().any(|(_, _, b)| String::from_utf8_lossy(b).contains("\"offset\":2")) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    engine.stop_webhook_worker(token);
    let paths = rec.lock().unwrap().iter().map(|(_, p, _)| p.clone()).collect();
    let _ = std::fs::remove_dir_all(&dir);
    paths
}

#[tokio::test]
async fn webhook_reply_method_is_executed_in_the_background() {
    let paths = run_reply_case("wh_reply_ok", br#"{"method":"sendMessage","chat_id":1,"text":"hi"}"#.to_vec()).await;
    assert!(paths.iter().any(|p| p.to_ascii_lowercase().contains("/sendmessage")), "{paths:?}");
    assert!(paths.iter().any(|p| p.contains("/getUpdates")));
}

#[tokio::test]
async fn oversized_webhook_reply_is_ignored() {
    let mut big = br#"{"method":"sendMessage","chat_id":1,"text":""#.to_vec();
    big.extend(std::iter::repeat_n(b'a', MAX_REPLY_BYTES + 4096));
    big.extend_from_slice(br#""}"#);
    let paths = run_reply_case("wh_reply_big", big).await;
    assert!(paths.iter().any(|p| p.contains("/getUpdates")));
    assert!(!paths.iter().any(|p| p.to_ascii_lowercase().contains("/sendmessage")), "oversized reply must not be executed: {paths:?}");
}

#[tokio::test]
async fn poll_errors_are_recorded_and_conflict_reissues_delete_webhook() {
    let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = mock_upstream(rec.clone(), |path| {
        if path.contains("/getUpdates") {
            (
                "409 Conflict",
                r#"{"ok":false,"error_code":409,"description":"Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first"}"#.to_string(),
            )
        } else {
            ok_upstream(path)
        }
    })
    .await;
    let (engine, dir) = test_engine("wh_409", port);
    let token = "123456:CONF";
    engine.start_webhook_worker(cfg(token, "http://127.0.0.1:9/hook".into()));
    let count_deletes = || rec.lock().unwrap().iter().filter(|(_, p, _)| p.contains("/deleteWebhook")).count();
    for _ in 0..100 {
        if count_deletes() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(count_deletes(), 1, "deleteWebhook must be re-issued");
    let (_, date, msg) = engine.webhook_states.get(token).unwrap().snapshot();
    assert!(date.is_some());
    assert!(msg.unwrap().contains("409"));
    // rate limited: more polls happen but there is no second repair within the interval
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(count_deletes(), 1);
    engine.stop_webhook_worker(token);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn fatal_poll_error_deletes_stored_record() {
    let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = mock_upstream(rec.clone(), |path| {
        if path.contains("/getUpdates") {
            ("401 Unauthorized", r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#.to_string())
        } else {
            ok_upstream(path)
        }
    })
    .await;
    let (engine, dir) = test_engine("wh_fatal", port);
    let token = "123456:DEAD";
    engine.storage.save_webhook(token, "http://127.0.0.1:9/hook", None, 40, None, None).await;
    engine.start_webhook_worker(cfg(token, "http://127.0.0.1:9/hook".into()));
    for _ in 0..100 {
        if !engine.webhook_workers.contains_key(token) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(engine.storage.get_webhook(token).await.is_none(), "dead bot must not be restored on every boot");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_starts_leave_exactly_one_worker() {
    let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
    let port = mock_upstream(rec.clone(), ok_upstream).await;
    let (engine, dir) = test_engine("wh_race", port);
    let token = "123456:RACE";
    let mut hs = Vec::new();
    for _ in 0..16 {
        let e = engine.clone();
        hs.push(tokio::spawn(async move { e.start_webhook_worker(cfg(token, "http://127.0.0.1:9/hook".into())) }));
    }
    for h in hs {
        h.await.unwrap();
    }
    assert_eq!(engine.webhook_workers.len(), 1);
    assert_eq!(engine.webhook_states.len(), 1);
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Every loser was aborted: a single poll loop makes one request per ~200ms mock delay.
    let before = rec.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let polls = rec.lock().unwrap().len() - before;
    assert!(polls <= 8, "orphaned workers keep polling: {polls} polls/s");
    engine.stop_webhook_worker(token);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn set_webhook_throttle_one_second() {
    let (engine, dir) = test_engine("wh_thr", 1);
    assert!(engine.check_set_webhook_throttle("1:a").is_ok());
    match engine.check_set_webhook_throttle("1:a") {
        Err(TelegramError::ApiError { code: 429, parameters, .. }) => {
            assert_eq!(parameters.unwrap().retry_after, Some(1));
        }
        other => panic!("expected 429, got {other:?}"),
    }
    assert!(engine.check_set_webhook_throttle("2:b").is_ok());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn worker_exit_on_401_clears_registry() {
    let up = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let up_port = up.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = up.accept().await.unwrap();
            tokio::spawn(async move {
                let _ = read_request(&mut s).await;
                respond(&mut s, "401 Unauthorized", r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#).await;
            });
        }
    });
    let (engine, dir) = test_engine("wh_401", up_port);
    engine.start_webhook_worker(cfg("123456:XYZ", "http://127.0.0.1:9/hook".into()));
    for _ in 0..100 {
        if !engine.webhook_workers.contains_key("123456:XYZ") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!engine.webhook_workers.contains_key("123456:XYZ"));
    assert!(!engine.webhook_states.contains_key("123456:XYZ"));
    let _ = std::fs::remove_dir_all(&dir);
}

type HeldCalls = Arc<(AtomicUsize, AtomicUsize)>;

/// Upstream whose getUpdates serves `n` single-update chats once; every other call (the webhook
/// reply methods) is accepted and then held open until the client hangs up. Returns the port and
/// the `(currently open, maximum open)` counters of held calls.
async fn holding_upstream(n: i64) -> (u16, HeldCalls) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let counters: HeldCalls = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
    let c = counters.clone();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = l.accept().await.unwrap();
            let c = c.clone();
            tokio::spawn(async move {
                let Some((head, body)) = read_request(&mut s).await else { return };
                if head.contains("/getUpdates") {
                    let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let off = v["offset"].as_i64().unwrap_or(0);
                    let limit = v["limit"].as_i64().unwrap_or(100);
                    let ups: Vec<Value> = (1..=n)
                        .filter(|id| *id >= off)
                        .take(limit as usize)
                        .map(|id| serde_json::json!({"update_id": id, "message": {"chat": {"id": id}}}))
                        .collect();
                    if ups.is_empty() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    respond(&mut s, "200 OK", &serde_json::json!({"ok": true, "result": ups}).to_string()).await;
                } else {
                    let open = c.0.fetch_add(1, Ordering::SeqCst) + 1;
                    c.1.fetch_max(open, Ordering::SeqCst);
                    let mut buf = [0u8; 64];
                    while matches!(s.read(&mut buf).await, Ok(n) if n > 0) {}
                    c.0.fetch_sub(1, Ordering::SeqCst);
                }
            });
        }
    });
    (port, counters)
}

#[tokio::test]
async fn reply_calls_are_bounded_and_cancelled_with_the_worker() {
    // 400 updates, each answered by the webhook with a method call that upstream never answers:
    // more than the executor (32 running + 256 queued) can take.
    let (up, held) = holding_upstream(400).await;
    let wh = reply_webhook(br#"{"method":"sendMessage","chat_id":1,"text":"hi"}"#.to_vec()).await;
    let (engine, dir) = test_engine("wh_reply_bound", up);
    let token = "123456:BOUND";
    engine.start_webhook_worker(cfg(token, format!("http://127.0.0.1:{wh}/hook")));

    // every update gets delivered (the held reply calls never delay delivery) ...
    let mut pending = i64::MAX;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (p, _, _) = engine.webhook_states.get(token).unwrap().snapshot();
        pending = p;
        if held.0.load(Ordering::SeqCst) >= 32 && p == 0 {
            break;
        }
    }
    assert_eq!(pending, 0, "delivery must not wait for reply calls");
    tokio::time::sleep(Duration::from_millis(300)).await;
    // ... while reply calls never exceed the concurrency cap
    assert_eq!(held.0.load(Ordering::SeqCst), 32);
    assert_eq!(held.1.load(Ordering::SeqCst), 32, "reply calls must stay bounded");

    // stopping the worker cancels the calls in flight (the connections get closed)
    engine.stop_webhook_worker(token);
    for _ in 0..100 {
        if held.0.load(Ordering::SeqCst) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(held.0.load(Ordering::SeqCst), 0, "reply calls must not outlive the worker");
    let _ = std::fs::remove_dir_all(&dir);
}

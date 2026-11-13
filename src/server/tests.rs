//! Golden tests of the whole HTTP relay: real router, real engine, recording mock upstream.
//! They pin status codes, headers, JSON bodies and what is forwarded upstream.

use crate::client::test_support::{Got, MockUpstream, Reply, TestApp, TOKEN};

const USER: &str = r#"{"id":123456,"is_bot":true,"first_name":"Bot","username":"bot_bot","can_join_groups":true,"can_read_all_group_messages":false,"supports_inline_queries":false}"#;

fn ok(result: &str) -> Reply {
    Reply::json(200, &format!(r#"{{"ok":true,"result":{result}}}"#))
}

async fn echo_upstream() -> MockUpstream {
    MockUpstream::start(|r| {
        if r.target.ends_with("/getMe") {
            ok(USER)
        } else {
            ok(r#"{"message_id":7}"#)
        }
    })
    .await
}

#[tokio::test]
async fn health_and_fallback() {
    let up = echo_upstream().await;
    let app = TestApp::start("health", false, &up).await;
    let g = Got::from(app.http.get(format!("{}/health", app.base)).send().await.unwrap()).await;
    assert_eq!((g.status, g.text()), (200, "OK"));
    let g = Got::from(app.http.get(format!("{}/nothing/at/all/here", app.base)).send().await.unwrap()).await;
    assert_eq!(g.status, 404);
    assert_eq!(g.content_type.as_deref(), Some("application/json"));
    assert_eq!(g.text(), r#"{"ok":false,"error_code":404,"description":"Not Found: method not found"}"#);
}

#[tokio::test]
async fn get_me_is_golden_and_cached() {
    let up = echo_upstream().await;
    let app = TestApp::start("getme", false, &up).await;
    let want = format!(r#"{{"ok":true,"result":{USER}}}"#);
    for path in ["bot{T}/getMe", "{T}/getMe", "bot{T}/getme", "bot{T}/GETME"] {
        let url = format!("{}/{}", app.base, path.replace("{T}", TOKEN));
        let g = Got::from(app.http.get(&url).send().await.unwrap()).await;
        assert_eq!(g.status, 200, "{path}");
        assert_eq!(g.content_type.as_deref(), Some("application/json"));
        assert_eq!(g.text(), want, "{path}");
    }
    let g = Got::from(app.http.post(format!("{}/bot{TOKEN}/getMe", app.base)).send().await.unwrap()).await;
    assert_eq!(g.text(), want);
    assert_eq!(up.requests().len(), 1, "getMe is served from the session cache");
}

#[tokio::test]
async fn get_me_upstream_401_is_relayed_as_error() {
    let up = MockUpstream::start(|_| Reply::json(401, r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#)).await;
    let app = TestApp::start("getme401", false, &up).await;
    let g = Got::from(app.http.get(format!("{}/bot{TOKEN}/getMe", app.base)).send().await.unwrap()).await;
    assert_eq!(g.status, 401);
    assert_eq!(g.text(), r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#);
}

#[tokio::test]
async fn invalid_tokens_are_rejected_without_upstream_call() {
    let up = echo_upstream().await;
    let app = TestApp::start("badtoken", false, &up).await;
    for (path, status, desc) in [
        ("botabc/sendMessage", 401, "Unauthorized: invalid token specified"),
        ("bot0123:abc/sendMessage", 401, "Unauthorized: invalid token specified"),
        ("bot+1:abc/sendMessage", 421, "Misdirected Request: forbidden token specified"),
        ("botabc/getMe", 401, "Unauthorized: invalid token specified"),
        ("bot12x:abc/getMe", 421, "Misdirected Request: forbidden token specified"),
    ] {
        let g = Got::from(app.http.post(format!("{}/{path}", app.base)).send().await.unwrap()).await;
        assert_eq!(g.status, status, "{path}");
        assert_eq!(
            g.text(),
            format!(r#"{{"ok":false,"error_code":{status},"description":"{desc}"}}"#),
            "{path}"
        );
    }
    assert!(up.requests().is_empty());
}

#[tokio::test]
async fn status_and_retry_after_come_from_the_reply_body() {
    let cases: [(&str, u16, Option<&str>); 7] = [
        (r#"{"ok":true,"result":1}"#, 200, None),
        (r#"{"ok":false,"error_code":400,"description":"Bad Request: x"}"#, 400, None),
        (r#"{"ok":false,"error_code":403,"description":"Forbidden: x"}"#, 403, None),
        (
            r#"{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 9","parameters":{"retry_after":9}}"#,
            429,
            Some("9"),
        ),
        (r#"{"ok":false,"description":"no code"}"#, 400, None),
        ("<html>not json</html>", 200, None),
        (r#"{"result":1}"#, 200, None),
    ];
    for (i, (body, status, retry)) in cases.into_iter().enumerate() {
        // The upstream HTTP status is irrelevant for the JSON relay: only the body counts.
        let up = MockUpstream::start(move |_| Reply::json(200, body)).await;
        let app = TestApp::start(&format!("status{i}"), false, &up).await;
        let g = Got::from(
            app.http
                .post(format!("{}/bot{TOKEN}/sendMessage", app.base))
                .header("content-type", "application/json")
                .body(r#"{"chat_id":1,"text":"hi"}"#)
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(g.status, status, "{body}");
        assert_eq!(g.retry_after.as_deref(), retry, "{body}");
        assert_eq!(g.text(), body, "reply passes through byte for byte");
        assert_eq!(g.content_type.as_deref(), Some("application/json"));
    }
}

#[tokio::test]
async fn requests_are_forwarded_unchanged() {
    let up = echo_upstream().await;
    let app = TestApp::start("forward", false, &up).await;
    let c = &app.http;
    let base = &app.base;

    // JSON body + query string
    c.post(format!("{base}/bot{TOKEN}/sendMessage?x=1&y=%20z"))
        .header("content-type", "application/json; charset=utf-8")
        .body(r#"{"chat_id":1,"text":"hi"}"#)
        .send()
        .await
        .unwrap();
    // token without the bot prefix
    c.post(format!("{base}/{TOKEN}/sendMessage"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("chat_id=1&text=hi")
        .send()
        .await
        .unwrap();
    // GET is relayed as a POST with the query string and no body
    c.get(format!("{base}/bot{TOKEN}/sendMessage?chat_id=1&text=hi")).send().await.unwrap();
    // body without content type: defaults to JSON
    c.post(format!("{base}/bot{TOKEN}/SendMessage")).body(r#"{"a":1}"#).send().await.unwrap();
    // multipart passes through untouched
    let mp = "--B\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n1\r\n--B--\r\n";
    c.post(format!("{base}/bot{TOKEN}/sendMessage"))
        .header("content-type", "multipart/form-data; boundary=B")
        .body(mp)
        .send()
        .await
        .unwrap();

    let r = up.requests();
    assert_eq!(r.len(), 5);
    assert_eq!(r[0].method, "POST");
    assert_eq!(r[0].target, format!("/bot{TOKEN}/sendMessage?x=1&y=%20z"));
    assert_eq!(r[0].content_type.as_deref(), Some("application/json; charset=utf-8"));
    assert_eq!(&r[0].body[..], br#"{"chat_id":1,"text":"hi"}"#);
    assert_eq!(r[1].target, format!("/bot{TOKEN}/sendMessage"));
    assert_eq!(r[1].content_type.as_deref(), Some("application/x-www-form-urlencoded"));
    assert_eq!(&r[1].body[..], b"chat_id=1&text=hi");
    assert_eq!(r[2].method, "POST");
    assert_eq!(r[2].target, format!("/bot{TOKEN}/sendMessage?chat_id=1&text=hi"));
    assert_eq!(r[2].content_type, None);
    assert!(r[2].body.is_empty());
    assert_eq!(r[3].target, format!("/bot{TOKEN}/SendMessage"));
    assert_eq!(r[3].content_type.as_deref(), Some("application/json"));
    assert_eq!(r[4].content_type.as_deref(), Some("multipart/form-data; boundary=B"));
    assert_eq!(&r[4].body[..], mp.as_bytes());
}

#[tokio::test]
async fn lifecycle_and_webhook_methods_are_golden() {
    let up = MockUpstream::start(|r| {
        if r.target.contains("/logOut") || r.target.contains("/deleteWebhook") {
            ok("true")
        } else if r.target.contains("/getWebhookInfo") {
            ok(r#"{"url":"https://example.com/h","has_custom_certificate":false,"pending_update_count":3}"#)
        } else if r.target.contains("/setWebhook") {
            ok("true")
        } else {
            ok(USER)
        }
    })
    .await;
    let app = TestApp::start("lifecycle", false, &up).await;
    let (c, base) = (&app.http, &app.base);

    let g = Got::from(c.post(format!("{base}/bot{TOKEN}/close")).send().await.unwrap()).await;
    assert_eq!((g.status, g.text()), (200, r#"{"ok":true,"result":true,"description":"Bot instance closed"}"#));
    let g = Got::from(c.get(format!("{base}/bot{TOKEN}/close")).send().await.unwrap()).await;
    assert_eq!((g.status, g.text()), (200, r#"{"ok":true,"result":true,"description":"Bot instance closed"}"#));
    assert!(up.requests().is_empty(), "close never reaches upstream");

    let g = Got::from(c.post(format!("{base}/bot{TOKEN}/logOut")).send().await.unwrap()).await;
    assert_eq!((g.status, g.text()), (200, r#"{"ok":true,"result":true}"#));

    let g = Got::from(c.post(format!("{base}/bot{TOKEN}/deleteWebhook?drop_pending_updates=true")).send().await.unwrap()).await;
    assert_eq!((g.status, g.text()), (200, r#"{"ok":true,"result":true,"description":"Webhook was deleted"}"#));
    let g = Got::from(c.post(format!("{base}/bot{TOKEN}/deleteWebhook")).send().await.unwrap()).await;
    assert_eq!(g.status, 200);

    let g = Got::from(c.get(format!("{base}/bot{TOKEN}/getWebhookInfo")).send().await.unwrap()).await;
    assert_eq!(
        (g.status, g.text()),
        (200, r#"{"ok":true,"result":{"url":"https://example.com/h","has_custom_certificate":false,"pending_update_count":3}}"#)
    );

    let g = Got::from(
        c.post(format!("{base}/bot{TOKEN}/setWebhook"))
            .header("content-type", "application/json")
            .body(r#"{"url":"https://example.com/hook","secret_token":"abc"}"#)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!((g.status, g.text()), (200, r#"{"ok":true,"result":true,"description":"Webhook was set"}"#));

    let r = up.requests();
    let targets: Vec<&str> = r.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        [
            format!("/bot{TOKEN}/logOut"),
            format!("/bot{TOKEN}/deleteWebhook?drop_pending_updates=true"),
            format!("/bot{TOKEN}/deleteWebhook"),
            format!("/bot{TOKEN}/getWebhookInfo"),
            format!("/bot{TOKEN}/setWebhook"),
        ]
    );
    // the cloud setWebhook request is forwarded as the client sent it
    assert_eq!(r[4].content_type.as_deref(), Some("application/json"));
    assert_eq!(&r[4].body[..], br#"{"url":"https://example.com/hook","secret_token":"abc"}"#);
}

#[tokio::test]
async fn webhook_errors_map_to_status_codes() {
    let up = MockUpstream::start(|_| {
        Reply::json(400, r#"{"ok":false,"error_code":400,"description":"Bad Request: nope"}"#)
    })
    .await;
    let app = TestApp::start("whErr", false, &up).await;
    for m in ["deleteWebhook", "getWebhookInfo"] {
        let g = Got::from(app.http.post(format!("{}/bot{TOKEN}/{m}", app.base)).send().await.unwrap()).await;
        assert_eq!(g.status, 400, "{m}");
        assert_eq!(g.text(), r#"{"ok":false,"error_code":400,"description":"Bad Request: nope"}"#, "{m}");
    }
    let g = Got::from(app.http.post(format!("{}/bot{TOKEN}/setWebhook", app.base)).body("url=https://example.com/x").send().await.unwrap()).await;
    assert_eq!(g.status, 400);
}

#[tokio::test]
async fn get_file_relayed_in_non_local_mode() {
    let up = MockUpstream::start(|r| {
        if r.target.contains("file_id=good") {
            ok(r#"{"file_id":"good","file_unique_id":"u","file_size":10,"file_path":"documents/f.bin"}"#)
        } else if r.target.contains("file_id=big") {
            ok(r#"{"file_id":"big","file_unique_id":"u","file_size":21000000,"file_path":"documents/b.bin"}"#)
        } else {
            Reply::json(400, r#"{"ok":false,"error_code":400,"description":"Bad Request: invalid file_id"}"#)
        }
    })
    .await;
    let app = TestApp::start("getfile", false, &up).await;
    let (c, base) = (&app.http, &app.base);

    let g = Got::from(c.get(format!("{base}/bot{TOKEN}/getFile?file_id=good")).send().await.unwrap()).await;
    assert_eq!(
        (g.status, g.text()),
        (200, r#"{"ok":true,"result":{"file_id":"good","file_unique_id":"u","file_size":10,"file_path":"documents/f.bin"}}"#)
    );
    // file_id in a JSON body and in a form body
    let g = Got::from(
        c.post(format!("{base}/bot{TOKEN}/getFile")).header("content-type", "application/json").body(r#"{"file_id":"good"}"#).send().await.unwrap(),
    )
    .await;
    assert_eq!(g.status, 200);
    let g = Got::from(
        c.post(format!("{base}/bot{TOKEN}/getFile")).header("content-type", "application/x-www-form-urlencoded").body("file_id=good").send().await.unwrap(),
    )
    .await;
    assert_eq!(g.status, 200);

    let g = Got::from(c.get(format!("{base}/bot{TOKEN}/getFile?file_id=bad")).send().await.unwrap()).await;
    assert_eq!(g.status, 400);
    assert_eq!(
        g.text(),
        // serde_json::json! sorts keys: the field order here is pinned on purpose.
        r#"{"description":"Bad Request: invalid file_id","error_code":400,"ok":false,"parameters":null}"#
    );
    // > 20 MB is rejected in non-local mode (C++ Client.cpp)
    let g = Got::from(c.get(format!("{base}/bot{TOKEN}/getFile?file_id=big")).send().await.unwrap()).await;
    assert_eq!(g.status, 400);
    assert!(g.text().contains("file is too big"), "{}", g.text());
    // without a file_id the request is relayed like any other method
    let before = up.requests().len();
    let _ = c.get(format!("{base}/bot{TOKEN}/getFile")).send().await.unwrap();
    assert_eq!(up.requests().len(), before + 1);
}

#[tokio::test]
async fn file_download_streams_from_upstream() {
    let up = MockUpstream::start(|r| {
        if r.target.ends_with("/documents/f.bin") {
            Reply::bytes(200, b"0123456789".to_vec())
        } else {
            Reply::bytes(404, b"nope".to_vec())
        }
    })
    .await;
    let app = TestApp::start("download", false, &up).await;
    let g = Got::from(app.http.get(format!("{}/file/bot{TOKEN}/documents/f.bin", app.base)).send().await.unwrap()).await;
    assert_eq!((g.status, g.body.as_slice()), (200, &b"0123456789"[..]));
    assert_eq!(g.content_type.as_deref(), Some("application/octet-stream"));
    assert_eq!(up.requests()[0].target, format!("/file/bot{TOKEN}/documents/f.bin"));

    let g = Got::from(app.http.get(format!("{}/file/bot{TOKEN}/documents/missing.bin", app.base)).send().await.unwrap()).await;
    assert_eq!(g.status, 404);
    assert_eq!(g.text(), r#"{"ok":false,"error_code":404,"description":"Not Found: File download failed: status 404 Not Found"}"#);
}

#[tokio::test]
async fn local_mode_serves_files_with_ranges() {
    let up = echo_upstream().await;
    let app = TestApp::start("ranges", true, &up).await;
    let dir = app.dir.join(TOKEN).join("documents");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("f.txt"), b"0123456789").unwrap();
    let url = format!("{}/file/bot{TOKEN}/documents/f.txt", app.base);

    let g = Got::from(app.http.get(&url).send().await.unwrap()).await;
    assert_eq!((g.status, g.body.as_slice()), (200, &b"0123456789"[..]));
    assert_eq!(g.content_type.as_deref(), Some("text/plain"));

    for (range, status, body, cr) in [
        ("bytes=2-5", 206, "2345", "bytes 2-5/10"),
        ("bytes=7-", 206, "789", "bytes 7-9/10"),
        ("bytes=-3", 206, "789", "bytes 7-9/10"),
        ("bytes=5-500", 206, "56789", "bytes 5-9/10"),
        ("bytes=10-", 416, "", "bytes */10"),
    ] {
        let resp = app.http.get(&url).header("range", range).send().await.unwrap();
        let got_cr = resp.headers().get("content-range").map(|v| v.to_str().unwrap().to_string());
        let g = Got::from(resp).await;
        assert_eq!((g.status, g.text()), (status, body), "{range}");
        assert_eq!(got_cr.as_deref(), Some(cr), "{range}");
    }
    assert!(up.requests().is_empty());
}

#[tokio::test]
async fn local_mode_get_file_downloads_once_and_caches() {
    let up = MockUpstream::start(|r| {
        if r.target.starts_with("/file/") {
            Reply::bytes(200, b"payload".to_vec())
        } else {
            ok(r#"{"file_id":"abc","file_unique_id":"u","file_size":7,"file_path":"documents/f.bin"}"#)
        }
    })
    .await;
    let app = TestApp::start("localgetfile", true, &up).await;
    let url = format!("{}/bot{TOKEN}/getFile?file_id=abc", app.base);
    let g1 = Got::from(app.http.get(&url).send().await.unwrap()).await;
    let want_path = app.dir.join(TOKEN).join("documents/f.bin");
    assert_eq!(
        g1.text(),
        format!(
            r#"{{"ok":true,"result":{{"file_id":"abc","file_unique_id":"u","file_size":7,"file_path":"{}"}}}}"#,
            want_path.display()
        )
    );
    assert_eq!(std::fs::read(&want_path).unwrap(), b"payload");
    assert_eq!(up.requests().len(), 2);
    let g2 = Got::from(app.http.get(&url).send().await.unwrap()).await;
    assert_eq!(g2.text(), g1.text());
    assert_eq!(up.requests().len(), 2, "second getFile is served from the cache");
}

/// Upstream whose file download is cut off after 10 of 100 bytes; getFile answers normally.
async fn truncating_upstream() -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let (mut s, _) = l.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = s.read(&mut buf).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]).to_string();
                let resp = if head.starts_with("GET /file/") {
                    "HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n0123456789".to_string()
                } else {
                    let body = r#"{"ok":true,"result":{"file_id":"abc","file_unique_id":"u","file_size":100,"file_path":"documents/f.bin"}}"#;
                    format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
                };
                let _ = s.write_all(resp.as_bytes()).await;
                let _ = s.shutdown().await;
            });
        }
    });
    url
}

#[tokio::test]
async fn local_mode_interrupted_download_is_neither_served_nor_cached() {
    let upstream_url = truncating_upstream().await;
    let app = TestApp::start_at("truncated", true, &upstream_url).await;
    let g = Got::from(app.http.get(format!("{}/bot{TOKEN}/getFile?file_id=abc", app.base)).send().await.unwrap()).await;
    assert_eq!(g.status, 200);
    // the client still gets the upstream (relative) path, not a local one
    assert!(g.text().contains(r#""file_path":"documents/f.bin""#), "{}", g.text());
    let docs = app.dir.join(TOKEN).join("documents");
    let leftovers: Vec<_> = std::fs::read_dir(&docs).map(|d| d.filter_map(Result::ok).map(|e| e.file_name()).collect()).unwrap_or_default();
    assert!(leftovers.is_empty(), "no partial file may remain: {leftovers:?}");
    assert!(app.engine.storage.get_cached_file("abc").await.is_none());
}

#[tokio::test]
async fn local_mode_json_with_local_path_becomes_multipart() {
    let up = echo_upstream().await;
    let app = TestApp::start("localmedia", true, &up).await;
    let pic = app.dir.join("pic.jpg");
    std::fs::write(&pic, b"JPEGDATA").unwrap();
    let (c, base) = (&app.http, &app.base);

    // plain JSON without any local path stays JSON
    c.post(format!("{base}/bot{TOKEN}/sendMessage"))
        .header("content-type", "application/json")
        .body(r#"{"chat_id":1,"text":"a/b"}"#)
        .send()
        .await
        .unwrap();
    // sendPhoto with a local file
    let body = format!(r#"{{"chat_id":5,"photo":"{}","caption":"cap","disable_notification":true,"reply_markup":{{"x":1}}}}"#, pic.display());
    let g = Got::from(c.post(format!("{base}/bot{TOKEN}/sendPhoto")).header("content-type", "application/json").body(body).send().await.unwrap()).await;
    assert_eq!(g.status, 200);
    // sendMediaGroup with a local file
    let body = format!(r#"{{"chat_id":5,"media":[{{"type":"photo","media":"{}"}},{{"type":"photo","media":"AgACfileid"}}]}}"#, pic.display());
    c.post(format!("{base}/bot{TOKEN}/sendMediaGroup")).header("content-type", "application/json").body(body).send().await.unwrap();
    // editMessageMedia with a local file
    let body = format!(r#"{{"chat_id":5,"message_id":2,"media":{{"type":"photo","media":"{}"}}}}"#, pic.display());
    c.post(format!("{base}/bot{TOKEN}/editMessageMedia")).header("content-type", "application/json").body(body).send().await.unwrap();

    let r = up.requests();
    assert_eq!(r.len(), 4);
    assert_eq!(r[0].content_type.as_deref(), Some("application/json"));
    assert_eq!(&r[0].body[..], br#"{"chat_id":1,"text":"a/b"}"#);

    let text = |i: usize| String::from_utf8_lossy(&r[i].body).to_string();
    for (i, req) in r.iter().enumerate().skip(1) {
        assert!(req.content_type.as_deref().unwrap().starts_with("multipart/form-data; boundary="), "{i}");
        assert!(text(i).contains("JPEGDATA"), "{i}");
        assert!(text(i).contains(r#"filename="pic.jpg""#), "{i}");
    }
    let t = text(1);
    assert!(t.contains("name=\"photo\""));
    assert!(t.contains("name=\"chat_id\"\r\n\r\n5\r\n"));
    assert!(t.contains("name=\"caption\"\r\n\r\ncap\r\n"));
    assert!(t.contains("name=\"disable_notification\"\r\n\r\ntrue\r\n"));
    assert!(t.contains("name=\"reply_markup\"\r\n\r\n{\"x\":1}\r\n"));
    let t = text(2);
    assert!(t.contains("name=\"file_0\""));
    assert!(t.contains(r#"[{"media":"attach://file_0","type":"photo"},{"media":"AgACfileid","type":"photo"}]"#));
    let t = text(3);
    assert!(t.contains("name=\"file_edit\""));
    assert!(t.contains(r#"{"media":"attach://file_edit","type":"photo"}"#));
}

#[tokio::test]
async fn local_mode_every_local_file_of_a_send_is_uploaded_with_it() {
    let up = echo_upstream().await;
    let app = TestApp::start("localextra", true, &up).await;
    let (video, thumb, cover) = (app.dir.join("v.mp4"), app.dir.join("v.thumb.jpg"), app.dir.join("v.cover.jpg"));
    std::fs::write(&video, b"VIDEODATA").unwrap();
    std::fs::write(&thumb, b"THUMBDATA").unwrap();
    std::fs::write(&cover, b"COVERDATA").unwrap();

    // The bot's sendVideo: the video, its thumbnail and its cover are all files on this disk.
    let body = format!(
        r#"{{"chat_id":5,"video":"file://{}","thumbnail":"file://{}","cover":"file://{}","duration":3,"supports_streaming":true}}"#,
        video.display(),
        thumb.display(),
        cover.display()
    );
    let g = Got::from(
        app.http.post(format!("{}/bot{TOKEN}/sendVideo", app.base)).header("content-type", "application/json").body(body).send().await.unwrap(),
    )
    .await;
    assert_eq!(g.status, 200);

    let r = up.requests();
    assert_eq!(r.len(), 1);
    let t = String::from_utf8_lossy(&r[0].body).to_string();
    assert!(r[0].content_type.as_deref().unwrap().starts_with("multipart/form-data; boundary="));
    // Three file parts, each with its bytes ...
    for (name, data) in [("video", "VIDEODATA"), ("attach_thumbnail", "THUMBDATA"), ("attach_cover", "COVERDATA")] {
        assert!(t.contains(&format!("name=\"{name}\"")), "{name}");
        assert!(t.contains(data), "{data}");
    }
    // ... the fields refer to the parts, and no `file://` path reaches the cloud.
    assert!(t.contains("name=\"thumbnail\"\r\n\r\nattach://attach_thumbnail\r\n"));
    assert!(t.contains("name=\"cover\"\r\n\r\nattach://attach_cover\r\n"));
    assert!(t.contains("name=\"duration\"\r\n\r\n3\r\n"));
    assert!(!t.contains("file://"), "{t}");
}

/// Sends an HTTP request whose body is cut off mid-stream: waits until the temp file of the large
/// upload exists, closes the connection, and expects the file to be gone afterwards.
async fn aborted_large_upload_leaves_nothing(app: &TestApp) {
    use tokio::io::AsyncWriteExt;
    let addr = app.base.trim_start_matches("http://").to_string();
    let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let head = format!(
        "POST /bot{TOKEN}/sendVideo HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=B\r\nContent-Length: 60000000\r\n\r\n"
    );
    let part = "--B\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n5\r\n--B\r\nContent-Disposition: form-data; name=\"video\"; filename=\"v.mp4\"\r\nContent-Type: video/mp4\r\n\r\n0123456789";
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(part.as_bytes()).await.unwrap();
    s.flush().await.unwrap();
    let tmp_files = || -> usize {
        std::fs::read_dir(&app.dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("mtproto_upload_"))
            .count()
    };
    for _ in 0..100 {
        if tmp_files() > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(tmp_files(), 1, "the large upload is being spooled to a temp file");
    // The connection dies in the middle of the file part.
    drop(s);
    for _ in 0..150 {
        if tmp_files() == 0 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("temp file of an aborted large upload was not removed");
}

async fn call_test_dc(status: u16, body: &'static str) -> (Got, Vec<String>) {
    let up = MockUpstream::start(move |_| {
        let mut r = Reply::json(status, body);
        r.headers.push(("retry-after", "7".to_string()));
        r
    })
    .await;
    let app = TestApp::start(&format!("testdc{status}"), false, &up).await;
    let resp = app.http.post(format!("{}/test/bot{TOKEN}/getMe?a=b", app.base)).body("x=1").send().await.unwrap();
    let got = Got::from(resp).await;
    (got, up.requests().into_iter().map(|r| r.target).collect())
}

#[tokio::test]
async fn test_dc_relay_uses_configured_upstream_and_passes_status() {
    let (g, paths) = call_test_dc(200, r#"{"ok":true,"result":1}"#).await;
    assert_eq!(g.status, 200);
    assert_eq!(paths, [format!("/test/bot{TOKEN}/getMe?a=b")]);

    let (g, _) = call_test_dc(
        429,
        r#"{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 7","parameters":{"retry_after":7}}"#,
    )
    .await;
    assert_eq!(g.status, 429);
    assert_eq!(g.retry_after.as_deref(), Some("7"));

    let (g, _) = call_test_dc(502, "<html>oops</html>").await;
    assert_eq!(g.status, 502);
    let j: serde_json::Value = serde_json::from_slice(&g.body).unwrap();
    assert_eq!((j["ok"].as_bool(), j["error_code"].as_i64()), (Some(false), Some(502)));
}

#[tokio::test]
async fn test_dc_files_come_from_the_configured_upstream() {
    let up = MockUpstream::start(|_| Reply::bytes(200, b"abc".to_vec())).await;
    let app = TestApp::start("testdcfile", false, &up).await;
    let g = Got::from(app.http.get(format!("{}/test/file/bot{TOKEN}/documents/f.bin", app.base)).send().await.unwrap()).await;
    assert_eq!((g.status, g.body.as_slice()), (200, &b"abc"[..]));
    assert_eq!(up.requests()[0].target, format!("/test/file/bot{TOKEN}/documents/f.bin"));
}

#[tokio::test]
async fn oversized_buffered_body_is_rejected_up_front() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let up = echo_upstream().await;
    let app = TestApp::start("bodylimit", false, &up).await;
    let addr = app.base.trim_start_matches("http://").to_string();
    let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let head = format!(
        "POST /bot{TOKEN}/sendMessage HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 70000000\r\nConnection: close\r\n\r\n"
    );
    s.write_all(head.as_bytes()).await.unwrap();
    let mut resp = Vec::new();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), s.read_to_end(&mut resp)).await;
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 413"), "{text}");
    assert!(text.contains(r#""description":"Request Entity Too Large""#), "{text}");
    assert!(up.requests().is_empty());
}

#[tokio::test]
async fn oversized_text_field_of_a_large_upload_is_rejected() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let up = echo_upstream().await;
    let app = TestApp::start("textlimit", false, &up).await;
    let addr = app.base.trim_start_matches("http://").to_string();
    let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let head = format!(
        "POST /bot{TOKEN}/sendVideo HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=B\r\nContent-Length: 60000000\r\nConnection: close\r\n\r\n"
    );
    s.write_all(head.as_bytes()).await.unwrap();
    s.write_all(b"--B\r\nContent-Disposition: form-data; name=\"caption\"\r\n\r\n").await.unwrap();
    // 2 MiB of caption, more than the 1 MiB limit; the server answers before it is all sent.
    for _ in 0..32 {
        if s.write_all(&[b'a'; 64 * 1024]).await.is_err() {
            break;
        }
    }
    let mut resp = Vec::new();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), s.read_to_end(&mut resp)).await;
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 400"), "{text}");
    assert!(text.contains("Multipart text field is too long"), "{text}");
}

#[tokio::test]
async fn aborted_large_multipart_upload_leaves_no_temp_file() {
    let up = echo_upstream().await;
    let app = TestApp::start("largeabort", false, &up).await;
    aborted_large_upload_leaves_nothing(&app).await;
}

#[tokio::test]
async fn upload_progress_is_served_while_an_upload_is_listed() {
    let up = echo_upstream().await;
    let app = TestApp::start("upProgress", false, &up).await;
    let path = "/tmp/upload-progress-http/x.mp4";
    let url = format!("{}/bot{TOKEN}/getUploadProgress?path=file://{path}", app.base);

    let g = Got::from(app.http.get(&url).send().await.unwrap()).await;
    assert_eq!(g.status, 404, "nothing is uploading");
    assert!(g.text().contains("no upload of this file is running"));

    let tracking = crate::mtproto::upload_progress::start(std::path::Path::new(path), 1000);
    tracking.entry().add(250);
    let g = Got::from(app.http.get(&url).send().await.unwrap()).await;
    assert_eq!(g.status, 200);
    assert_eq!(g.text(), r#"{"ok":true,"result":{"done_bytes":250,"total_bytes":1000}}"#);
    // POST with a JSON body works too, with or without `file://`.
    let g = Got::from(
        app.http
            .post(format!("{}/bot{TOKEN}/getUploadProgress", app.base))
            .header("content-type", "application/json")
            .body(format!(r#"{{"path":"{path}"}}"#))
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(g.status, 200);
    drop(tracking);
    let g = Got::from(app.http.get(&url).send().await.unwrap()).await;
    assert_eq!(g.status, 404, "gone once the upload ends");
}

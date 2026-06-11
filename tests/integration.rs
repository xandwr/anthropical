use anthropical::{Anthropic, Error};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Serve one canned response per accepted connection, capturing each raw
/// request (headers + body). An empty response string closes the connection
/// without replying, simulating a transport failure.
fn serve(responses: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let hits = Arc::new(AtomicUsize::new(0));
    let captured = requests.clone();
    let counter = hits.clone();
    thread::spawn(move || {
        for response in responses {
            let (mut sock, _) = listener.accept().unwrap();
            let request = drain_request(&mut sock);
            captured.lock().unwrap().push(request);
            counter.fetch_add(1, Ordering::SeqCst);
            sock.write_all(response.as_bytes()).unwrap();
        }
    });
    (url, requests, hits)
}

/// Read one full HTTP request (headers, then content-length body) and return
/// it verbatim.
fn drain_request(sock: &mut TcpStream) -> String {
    let mut reader = BufReader::new(sock);
    let mut request = String::new();
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        if let Some(rest) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = rest.trim().parse().unwrap();
        }
        let blank = line.trim_end().is_empty();
        request.push_str(&line);
        if blank {
            break;
        }
    }
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).unwrap();
    request.push_str(&String::from_utf8(body).unwrap());
    request
}

fn http(status: u16, headers: &[(&str, &str)], body: &str) -> String {
    let mut response = format!("HTTP/1.1 {status} X\r\n");
    for (name, value) in headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str(&format!(
        "content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    ));
    response
}

fn message_body() -> String {
    json!({
        "id": "msg_1",
        "model": "claude-opus-4-8",
        "role": "assistant",
        "content": [{"type": "text", "text": "pong"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 3, "output_tokens": 1}
    })
    .to_string()
}

fn ok_response() -> String {
    http(200, &[("request-id", "req_ok")], &message_body())
}

fn rate_limited(retry_after: &str) -> String {
    http(
        429,
        &[("retry-after", retry_after)],
        r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#,
    )
}

fn sse(records: &[Value]) -> String {
    records.iter().map(|r| format!("data: {r}\n\n")).collect()
}

#[test]
fn send_maps_success_and_request_id() {
    let (url, requests, _) = serve(vec![ok_response()]);
    let response = Anthropic::new("test-key")
        .base_url(url)
        .message("claude-opus-4-8")
        .user("ping")
        .send()
        .unwrap();

    assert_eq!(response.text(), "pong");
    assert_eq!(response.stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(response.request_id.as_deref(), Some("req_ok"));

    let request = &requests.lock().unwrap()[0];
    assert!(request.contains("POST /v1/messages HTTP/1.1"));
    assert!(request.contains("x-api-key: test-key"));
    assert!(request.contains("anthropic-version: 2023-06-01"));
}

#[test]
fn custom_headers_are_sent() {
    let (url, requests, _) = serve(vec![ok_response()]);
    Anthropic::new("k")
        .base_url(url)
        .message("m")
        .header("anthropic-beta", "files-api-2025-04-14")
        .user("x")
        .send()
        .unwrap();
    assert!(requests.lock().unwrap()[0].contains("anthropic-beta: files-api-2025-04-14"));
}

#[test]
fn api_errors_are_typed() {
    let body = r#"{"type":"error","error":{"type":"invalid_request_error","message":"bad"},"request_id":"req_err"}"#;
    let (url, _, _) = serve(vec![http(400, &[], body)]);
    let err = Anthropic::new("k")
        .base_url(url)
        .message("m")
        .user("x")
        .send()
        .unwrap_err();
    match err {
        Error::Api {
            status,
            kind,
            message,
            request_id,
            ..
        } => {
            assert_eq!(status, Some(400));
            assert_eq!(kind, "invalid_request_error");
            assert_eq!(message, "bad");
            assert_eq!(request_id.as_deref(), Some("req_err"));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn retries_a_429_then_succeeds() {
    let (url, _, hits) = serve(vec![rate_limited("0"), ok_response()]);
    let response = Anthropic::new("k")
        .base_url(url)
        .message("m")
        .user("x")
        .send()
        .unwrap();
    assert_eq!(response.text(), "pong");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[test]
fn retries_a_dropped_connection() {
    // First "response" is empty: the server accepts and closes without replying.
    let (url, _, hits) = serve(vec![String::new(), ok_response()]);
    let response = Anthropic::new("k")
        .base_url(url)
        .message("m")
        .user("x")
        .send()
        .unwrap();
    assert_eq!(response.text(), "pong");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[test]
fn exhausted_retries_surface_the_error_with_retry_after() {
    let (url, _, hits) = serve(vec![rate_limited("7")]);
    let err = Anthropic::new("k")
        .base_url(url)
        .max_retries(0)
        .message("m")
        .user("x")
        .send()
        .unwrap_err();
    match err {
        Error::Api {
            status,
            kind,
            retry_after,
            ..
        } => {
            assert_eq!(status, Some(429));
            assert_eq!(kind, "rate_limit_error");
            assert_eq!(retry_after, Some(Duration::from_secs(7)));
        }
        other => panic!("unexpected: {other:?}"),
    }
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn stream_collects_a_full_message() {
    let body = sse(&[
        json!({"type": "message_start", "message": {
            "id": "msg_s", "role": "assistant", "model": "claude-opus-4-8",
            "content": [], "usage": {"input_tokens": 5}
        }}),
        json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "streamed"}}),
        json!({"type": "content_block_stop", "index": 0}),
        json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 2}}),
        json!({"type": "message_stop"}),
    ]);
    let (url, requests, _) = serve(vec![http(
        200,
        &[("content-type", "text/event-stream"), ("request-id", "req_s")],
        &body,
    )]);

    let stream = Anthropic::new("k")
        .base_url(url)
        .message("m")
        .user("x")
        .stream()
        .unwrap();
    assert_eq!(stream.request_id(), Some("req_s"));

    let message = stream.collect_message().unwrap();
    assert_eq!(message.text(), "streamed");
    assert_eq!(message.stop_reason.as_deref(), Some("end_turn"));
    assert_eq!(message.usage.output_tokens, Some(2));
    assert_eq!(message.request_id.as_deref(), Some("req_s"));

    assert!(requests.lock().unwrap()[0].contains("\"stream\":true"));
}

#[test]
fn mid_stream_error_event_is_an_error() {
    let body = sse(&[
        json!({"type": "message_start", "message": {"id": "msg_e", "role": "assistant"}}),
        json!({"type": "error", "error": {"type": "overloaded_error", "message": "busy"}}),
    ]);
    let (url, _, _) = serve(vec![http(200, &[], &body)]);

    let err = Anthropic::new("k")
        .base_url(url)
        .message("m")
        .user("x")
        .stream()
        .unwrap()
        .collect_message()
        .unwrap_err();
    match err {
        Error::Api { status, kind, .. } => {
            assert_eq!(status, None);
            assert_eq!(kind, "overloaded_error");
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn truncated_stream_body_is_an_error() {
    // Promise more bytes than are sent, then close: a mid-stream disconnect.
    let body = sse(&[
        json!({"type": "message_start", "message": {"id": "msg_t", "role": "assistant"}}),
    ]);
    let response = format!(
        "HTTP/1.1 200 X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len() + 50
    );
    let (url, _, _) = serve(vec![response]);

    let err = Anthropic::new("k")
        .base_url(url)
        .message("m")
        .user("x")
        .stream()
        .unwrap()
        .collect_message()
        .unwrap_err();
    assert!(matches!(err, Error::Io(_) | Error::Transport(_)), "{err:?}");
}

#[test]
fn count_tokens_strips_generation_params() {
    let (url, requests, _) = serve(vec![http(200, &[], r#"{"input_tokens":42}"#)]);
    let count = Anthropic::new("k")
        .base_url(url)
        .message("m")
        .max_tokens(9000)
        .temperature(0.5)
        .user("x")
        .count_tokens()
        .unwrap();
    assert_eq!(count, 42);

    let request = &requests.lock().unwrap()[0];
    assert!(request.contains("POST /v1/messages/count_tokens HTTP/1.1"));
    assert!(!request.contains("max_tokens"));
    assert!(!request.contains("temperature"));
}

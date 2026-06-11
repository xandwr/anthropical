use crate::event::ParseError;
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;

/// Anything that can go wrong talking to the API.
#[derive(Debug, Error)]
pub enum Error {
    /// The API declined the request: over HTTP (`status` set), or as an
    /// `error` event mid-stream (`status` empty).
    #[error("api error{} ({kind}): {message}", status.map(|s| format!(" {s}")).unwrap_or_default())]
    Api {
        status: Option<u16>,
        /// The API's `error.type`, e.g. `"rate_limit_error"`.
        kind: String,
        message: String,
        /// The `request-id` the API assigned; quote it when reporting issues.
        request_id: Option<String>,
        /// Server-suggested wait before retrying, from `retry-after`.
        retry_after: Option<Duration>,
    },

    /// Transport failure (connection, TLS, timeout) before a response arrived.
    #[error("transport error: {0}")]
    Transport(#[from] ureq::Error),

    /// A response body we couldn't decode into the expected shape.
    #[error("decode error: {0}")]
    Decode(#[from] serde_json::Error),

    /// A failure while parsing the streaming event feed.
    #[error(transparent)]
    Stream(#[from] ParseError),

    /// I/O failure while reading a (streaming) response body.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("ANTHROPIC_API_KEY is not set")]
    MissingApiKey,
}

impl Error {
    /// Map a non-2xx response body into [`Error::Api`]. Falls back to the raw
    /// text if the body isn't the expected `{error: {type, message}}` shape.
    pub(crate) fn api(
        status: u16,
        text: &str,
        request_id: Option<String>,
        retry_after: Option<Duration>,
    ) -> Self {
        let body: Value = serde_json::from_str(text).unwrap_or(Value::Null);
        let err = body.get("error");
        let field = |v: Option<&Value>, key: &str| {
            v.and_then(|v| v.get(key))
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        Error::Api {
            status: Some(status),
            kind: field(err, "type").unwrap_or_else(|| "unknown".into()),
            message: field(err, "message").unwrap_or_else(|| text.to_string()),
            request_id: request_id.or_else(|| field(Some(&body), "request_id")),
            retry_after,
        }
    }

    /// Map a mid-stream `error` event payload into [`Error::Api`].
    pub(crate) fn api_event(error: &Value) -> Self {
        Error::Api {
            status: None,
            kind: error
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| error.to_string()),
            request_id: None,
            retry_after: None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_formed_error_body() {
        let body = r#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"},"request_id":"req_1"}"#;
        match Error::api(429, body, None, Some(Duration::from_secs(3))) {
            Error::Api {
                status,
                kind,
                message,
                request_id,
                retry_after,
            } => {
                assert_eq!(status, Some(429));
                assert_eq!(kind, "rate_limit_error");
                assert_eq!(message, "slow down");
                assert_eq!(request_id.as_deref(), Some("req_1"));
                assert_eq!(retry_after, Some(Duration::from_secs(3)));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn header_request_id_wins_over_body() {
        let body = r#"{"error":{"type":"x","message":"y"},"request_id":"req_body"}"#;
        match Error::api(400, body, Some("req_header".into()), None) {
            Error::Api { request_id, .. } => assert_eq!(request_id.as_deref(), Some("req_header")),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn garbage_body_falls_back_to_raw_text() {
        match Error::api(502, "<html>bad gateway</html>", None, None) {
            Error::Api { kind, message, .. } => {
                assert_eq!(kind, "unknown");
                assert_eq!(message, "<html>bad gateway</html>");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn stream_error_event_has_no_status() {
        let v = serde_json::json!({"type": "overloaded_error", "message": "busy"});
        match Error::api_event(&v) {
            Error::Api {
                status,
                kind,
                message,
                ..
            } => {
                assert_eq!(status, None);
                assert_eq!(kind, "overloaded_error");
                assert_eq!(message, "busy");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}

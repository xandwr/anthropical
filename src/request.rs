use crate::client::Anthropic;
use crate::error::{Error, Result};
use crate::response::Response;
use crate::stream::EventStream;
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::time::Duration;

/// One conversation turn in the request.
#[derive(Debug, Clone, Serialize)]
pub struct Msg {
    pub role: String,
    pub content: Content,
}

/// Message content: plain text, or a list of raw content blocks for anything
/// richer (tool results, images, echoed assistant turns).
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Blocks(Vec<Value>),
}

impl Msg {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".into(),
            content: Content::Text(content.into()),
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: "assistant".into(),
            content: Content::Text(content.into()),
        }
    }

    /// A user turn made of raw content blocks, e.g. [`tool_result`]s.
    pub fn user_blocks(blocks: impl IntoIterator<Item = Value>) -> Self {
        Self {
            role: "user".into(),
            content: Content::Blocks(blocks.into_iter().collect()),
        }
    }

    /// An assistant turn made of raw content blocks; see
    /// [`Response::to_msg`] for echoing a reply back in a tool-use loop.
    pub fn assistant_blocks(blocks: impl IntoIterator<Item = Value>) -> Self {
        Self {
            role: "assistant".into(),
            content: Content::Blocks(blocks.into_iter().collect()),
        }
    }
}

/// A `tool_result` content block answering the tool call with the given id.
pub fn tool_result(tool_use_id: impl Into<String>, content: impl Into<String>) -> Value {
    json!({
        "type": "tool_result",
        "tool_use_id": tool_use_id.into(),
        "content": content.into(),
    })
}

/// Like [`tool_result`], but marks the execution as failed so the model can
/// adapt rather than trusting the output.
pub fn tool_error(tool_use_id: impl Into<String>, content: impl Into<String>) -> Value {
    json!({
        "type": "tool_result",
        "tool_use_id": tool_use_id.into(),
        "content": content.into(),
        "is_error": true,
    })
}

/// Fluent builder for a Messages API request.
///
/// Borrowed from the [`Anthropic`] client; build it up with chained setters,
/// then finish with [`send`](Self::send) (blocking), [`stream`](Self::stream),
/// or [`count_tokens`](Self::count_tokens).
pub struct MessageBuilder<'a> {
    client: &'a Anthropic,
    model: String,
    max_tokens: u32,
    messages: Vec<Msg>,
    system: Option<String>,
    temperature: Option<f32>,
    headers: Vec<(String, String)>,
    /// Extra top-level fields merged into the request body verbatim — an
    /// escape hatch for parameters we don't (yet) have a typed setter for.
    extra: Map<String, Value>,
}

impl<'a> MessageBuilder<'a> {
    pub(crate) fn new(client: &'a Anthropic, model: String) -> Self {
        Self {
            client,
            model,
            max_tokens: 1024,
            messages: Vec::new(),
            system: None,
            temperature: None,
            headers: Vec::new(),
            extra: Map::new(),
        }
    }

    /// Maximum tokens to generate. Defaults to 1024.
    pub fn max_tokens(mut self, n: u32) -> Self {
        self.max_tokens = n;
        self
    }

    /// Set the system prompt.
    pub fn system(mut self, prompt: impl Into<String>) -> Self {
        self.system = Some(prompt.into());
        self
    }

    /// Sampling temperature.
    ///
    /// Removed from Opus 4.7+ and Fable 5, where setting it returns a 400;
    /// only use this when targeting older models.
    pub fn temperature(mut self, t: f32) -> Self {
        self.temperature = Some(t);
        self
    }

    /// Append a user turn.
    pub fn user(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Msg::user(content));
        self
    }

    /// Append an assistant turn.
    pub fn assistant(mut self, content: impl Into<String>) -> Self {
        self.messages.push(Msg::assistant(content));
        self
    }

    /// Append a pre-built turn (echoed responses, tool results).
    pub fn msg(mut self, msg: Msg) -> Self {
        self.messages.push(msg);
        self
    }

    /// Replace the entire message list (escape hatch for pre-built histories).
    pub fn messages(mut self, messages: impl IntoIterator<Item = Msg>) -> Self {
        self.messages = messages.into_iter().collect();
        self
    }

    /// Set an arbitrary top-level request field by name (e.g. `"tools"`,
    /// `"thinking"`, `"top_p"`). Escape hatch for anything without a setter;
    /// these win over the typed setters when the names collide.
    pub fn with(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.extra.insert(key.into(), value.into());
        self
    }

    /// Add a request header, e.g. `("anthropic-beta", ...)` for beta features.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Send the request and block until the full response arrives.
    pub fn send(self) -> Result<Response> {
        let body = self.body();
        let (text, request_id) = read_success(self.post("/v1/messages", &body, false)?)?;
        let mut message: Response = serde_json::from_str(&text)?;
        message.request_id = request_id;
        Ok(message)
    }

    /// Send the request and stream events as they arrive.
    pub fn stream(mut self) -> Result<EventStream> {
        self.extra.insert("stream".into(), Value::Bool(true));
        let body = self.body();
        let mut resp = self.post("/v1/messages", &body, true)?;
        let (status, request_id, retry_after) = response_meta(&resp);

        if !(200..300).contains(&status) {
            let text = resp.body_mut().read_to_string()?;
            return Err(Error::api(status, &text, request_id, retry_after));
        }

        Ok(EventStream::new(
            resp.into_body().into_reader(),
            request_id,
        ))
    }

    /// Count the input tokens this request would consume, without running it.
    pub fn count_tokens(self) -> Result<u64> {
        let mut body = self.body();
        if let Some(map) = body.as_object_mut() {
            // Generation-only parameters the count endpoint rejects.
            map.remove("max_tokens");
            map.remove("temperature");
            map.remove("stream");
        }
        let (text, _) = read_success(self.post("/v1/messages/count_tokens", &body, false)?)?;

        #[derive(serde::Deserialize)]
        struct Count {
            input_tokens: u64,
        }
        Ok(serde_json::from_str::<Count>(&text)?.input_tokens)
    }

    /// Build the JSON request body.
    fn body(&self) -> Value {
        let mut map = Map::new();
        map.insert("model".into(), Value::String(self.model.clone()));
        map.insert("max_tokens".into(), Value::from(self.max_tokens));
        map.insert(
            "messages".into(),
            serde_json::to_value(&self.messages).unwrap_or(Value::Null),
        );
        if let Some(system) = &self.system {
            map.insert("system".into(), Value::String(system.clone()));
        }
        if let Some(t) = self.temperature {
            map.insert("temperature".into(), Value::from(t));
        }
        for (k, v) in &self.extra {
            map.insert(k.clone(), v.clone());
        }
        Value::Object(map)
    }

    /// POST with retries: connection failures, 408/429, and 5xx are retried
    /// with exponential backoff, honoring `retry-after` when the server sends
    /// one. The last attempt's outcome is returned as-is.
    fn post(
        &self,
        path: &str,
        body: &Value,
        streaming: bool,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        let payload = serde_json::to_string(body)?;
        let mut attempt = 0;
        loop {
            match self.post_once(path, &payload, streaming) {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if attempt >= self.client.max_retries || !retryable_status(status) {
                        return Ok(resp);
                    }
                    let (_, _, retry_after) = response_meta(&resp);
                    std::thread::sleep(retry_delay(attempt, retry_after));
                }
                Err(Error::Transport(_)) if attempt < self.client.max_retries => {
                    std::thread::sleep(retry_delay(attempt, None));
                }
                Err(e) => return Err(e),
            }
            attempt += 1;
        }
    }

    /// Issue one POST with auth, version, and custom headers.
    ///
    /// Non-streaming calls get a global deadline (the server sends the body
    /// in one piece, so it bounds the whole call). Streaming calls bound only
    /// connect and time-to-response: a legitimate stream can run for minutes.
    fn post_once(
        &self,
        path: &str,
        payload: &str,
        streaming: bool,
    ) -> Result<ureq::http::Response<ureq::Body>> {
        let url = format!("{}{path}", self.client.base_url);
        let config = self
            .client
            .agent
            .post(&url)
            .config()
            .timeout_connect(Some(self.client.connect_timeout));
        let req = if streaming {
            config.timeout_recv_response(Some(self.client.timeout))
        } else {
            config.timeout_global(Some(self.client.timeout))
        };
        let mut req = req
            .build()
            .header("x-api-key", &self.client.api_key)
            .header("anthropic-version", &self.client.version)
            .header("content-type", "application/json");
        for (name, value) in &self.headers {
            req = req.header(name.as_str(), value.as_str());
        }
        Ok(req.send(payload)?)
    }
}

/// Status code, `request-id`, and `retry-after` from a response.
fn response_meta(resp: &ureq::http::Response<ureq::Body>) -> (u16, Option<String>, Option<Duration>) {
    let header = |name: &str| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let retry_after = header("retry-after")
        .and_then(|v| v.parse().ok())
        .map(Duration::from_secs);
    (resp.status().as_u16(), header("request-id"), retry_after)
}

/// Read the body, mapping a non-2xx status into a typed [`Error::Api`].
fn read_success(mut resp: ureq::http::Response<ureq::Body>) -> Result<(String, Option<String>)> {
    let (status, request_id, retry_after) = response_meta(&resp);
    let text = resp.body_mut().read_to_string()?;
    if !(200..300).contains(&status) {
        return Err(Error::api(status, &text, request_id, retry_after));
    }
    Ok((text, request_id))
}

fn retryable_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..)
}

/// Server-suggested wait when given, else exponential backoff (0.5s, 1s, 2s,
/// ...), both capped so a bad `retry-after` can't stall the caller.
fn retry_delay(attempt: u32, retry_after: Option<Duration>) -> Duration {
    const CAP: Duration = Duration::from_secs(30);
    let backoff = Duration::from_millis(500u64 << attempt.min(6));
    retry_after.unwrap_or(backoff).min(CAP)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_shape_and_extra_override() {
        let client = Anthropic::new("k");
        let body = client
            .message("model-x")
            .max_tokens(7)
            .system("sys")
            .user("hi")
            .assistant("yo")
            .with("max_tokens", 99)
            .body();
        assert_eq!(
            body,
            json!({
                "model": "model-x",
                "max_tokens": 99,
                "system": "sys",
                "messages": [
                    {"role": "user", "content": "hi"},
                    {"role": "assistant", "content": "yo"},
                ],
            })
        );
    }

    #[test]
    fn block_messages_serialize_as_arrays() {
        let msg = Msg::user_blocks([tool_result("tu_1", "ok"), tool_error("tu_2", "boom")]);
        assert_eq!(
            serde_json::to_value(&msg).unwrap(),
            json!({
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "tu_1", "content": "ok"},
                    {"type": "tool_result", "tool_use_id": "tu_2", "content": "boom", "is_error": true},
                ],
            })
        );
    }

    #[test]
    fn retry_delay_backs_off_honors_server_and_caps() {
        assert_eq!(retry_delay(0, None), Duration::from_millis(500));
        assert_eq!(retry_delay(3, None), Duration::from_secs(4));
        assert_eq!(retry_delay(0, Some(Duration::from_secs(2))), Duration::from_secs(2));
        assert_eq!(retry_delay(0, Some(Duration::from_secs(600))), Duration::from_secs(30));
        assert_eq!(retry_delay(63, None), Duration::from_secs(30));
    }

    #[test]
    fn retryable_statuses() {
        assert!(retryable_status(429));
        assert!(retryable_status(500));
        assert!(retryable_status(529));
        assert!(retryable_status(408));
        assert!(!retryable_status(400));
        assert!(!retryable_status(200));
    }
}

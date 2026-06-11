use crate::client::Anthropic;
use crate::error::{Error, Result};
use crate::parser::EventParser;
use crate::response::Response;
use crate::stream::EventStream;
use serde::Serialize;
use serde_json::Value;

/// One conversation turn in the request.
#[derive(Debug, Clone, Serialize)]
pub struct Msg {
    pub role: String,
    pub content: String,
}

impl Msg {
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".into(),
            content: content.into(),
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: "assistant".into(),
            content: content.into(),
        }
    }
}

/// Fluent builder for a Messages API request.
///
/// Borrowed from the [`Anthropic`] client; build it up with chained setters,
/// then finish with [`send`](Self::send) (blocking) or [`stream`](Self::stream).
pub struct MessageBuilder<'a> {
    client: &'a Anthropic,
    model: String,
    max_tokens: u32,
    messages: Vec<Msg>,
    system: Option<String>,
    temperature: Option<f32>,
    /// Extra top-level fields merged into the request body verbatim — an
    /// escape hatch for parameters we don't (yet) have a typed setter for.
    extra: serde_json::Map<String, Value>,
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
            extra: serde_json::Map::new(),
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

    /// Replace the entire message list (escape hatch for pre-built histories).
    pub fn messages(mut self, messages: impl IntoIterator<Item = Msg>) -> Self {
        self.messages = messages.into_iter().collect();
        self
    }

    /// Set an arbitrary top-level request field by name (e.g. `"tools"`,
    /// `"thinking"`, `"top_p"`). Escape hatch for anything without a setter.
    pub fn with(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.extra.insert(key.into(), value.into());
        self
    }

    /// Send the request and block until the full response arrives.
    pub fn send(self) -> Result<Response> {
        let body = self.body(false);
        let mut resp = self.post(&body)?;
        let status = resp.status().as_u16();
        let text = resp.body_mut().read_to_string()?;

        if !(200..300).contains(&status) {
            return Err(parse_api_error(status, &text));
        }
        Ok(serde_json::from_str(&text)?)
    }

    /// Send the request and stream events as they arrive.
    pub fn stream(mut self) -> Result<EventStream> {
        self.extra.insert("stream".into(), Value::Bool(true));
        let body = self.body(true);
        let mut resp = self.post(&body)?;
        let status = resp.status().as_u16();

        if !(200..300).contains(&status) {
            let text = resp.body_mut().read_to_string()?;
            return Err(parse_api_error(status, &text));
        }

        let reader = resp.into_body().into_reader();
        Ok(EventStream::new(reader, EventParser::new()))
    }

    /// Build the JSON request body.
    fn body(&self, _streaming: bool) -> Value {
        let mut map = serde_json::Map::new();
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

    /// Issue the POST with auth + version headers.
    fn post(&self, body: &Value) -> Result<ureq::http::Response<ureq::Body>> {
        let url = format!("{}/v1/messages", self.client.base_url);
        let payload = serde_json::to_string(body)?;
        let resp = self
            .client
            .agent
            .post(&url)
            .header("x-api-key", &self.client.api_key)
            .header("anthropic-version", &self.client.version)
            .header("content-type", "application/json")
            .send(&payload)?;
        Ok(resp)
    }
}

/// Map an API error response body into a typed [`Error::Api`]. Falls back to
/// the raw text if the body isn't the expected `{error: {type, message}}` shape.
fn parse_api_error(status: u16, text: &str) -> Error {
    let parsed: Option<Value> = serde_json::from_str(text).ok();
    let err = parsed.as_ref().and_then(|v| v.get("error"));
    let kind = err
        .and_then(|e| e.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let message = err
        .and_then(|e| e.get("message"))
        .and_then(Value::as_str)
        .unwrap_or(text)
        .to_string();
    Error::Api {
        status,
        kind,
        message,
    }
}

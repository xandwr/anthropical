use crate::error::{Error, Result};
use crate::request::MessageBuilder;
use std::time::Duration;

const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const API_VERSION: &str = "2023-06-01";

/// A handle to the Anthropic API.
///
/// Cheap to clone where you need to share it; holds a pooled connection agent
/// so repeated calls reuse TLS connections. All calls are blocking: in async
/// contexts, drive them from `spawn_blocking` or a dedicated thread.
#[derive(Clone)]
pub struct Anthropic {
    pub(crate) agent: ureq::Agent,
    pub(crate) api_key: String,
    pub(crate) base_url: String,
    pub(crate) version: String,
    pub(crate) timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) max_retries: u32,
}

impl Anthropic {
    /// Create a client with the given API key and sensible defaults.
    pub fn new(api_key: impl Into<String>) -> Self {
        // Let non-2xx responses come back as Ok so we can read the JSON error
        // body and map it into a typed `Error::Api` ourselves.
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();

        Self {
            agent,
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
            version: API_VERSION.to_string(),
            timeout: Duration::from_secs(600),
            connect_timeout: Duration::from_secs(10),
            max_retries: 2,
        }
    }

    /// Create a client from `ANTHROPIC_API_KEY`, honoring `ANTHROPIC_BASE_URL`
    /// when set.
    pub fn from_env() -> Result<Self> {
        let key = std::env::var("ANTHROPIC_API_KEY").map_err(|_| Error::MissingApiKey)?;
        let client = Self::new(key);
        match std::env::var("ANTHROPIC_BASE_URL") {
            Ok(url) => Ok(client.base_url(url)),
            Err(_) => Ok(client),
        }
    }

    /// Override the base URL (e.g. to point at a proxy or gateway).
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        self.base_url = url.trim_end_matches('/').to_string();
        self
    }

    /// Override the `anthropic-version` header.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Deadline for the server to respond: the whole call for
    /// [`send`](MessageBuilder::send), time-to-first-byte for
    /// [`stream`](MessageBuilder::stream) (the stream body itself is
    /// unbounded). Defaults to 10 minutes.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// TCP connect deadline. Defaults to 10 seconds.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Retries after connection failures, 408/429, and 5xx responses, with
    /// exponential backoff honoring `retry-after`. Defaults to 2.
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Start building a message request against the given model.
    pub fn message(&self, model: impl Into<String>) -> MessageBuilder<'_> {
        MessageBuilder::new(self, model.into())
    }
}

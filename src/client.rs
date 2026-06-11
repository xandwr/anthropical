use crate::request::MessageBuilder;

const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const API_VERSION: &str = "2023-06-01";

/// A handle to the Anthropic API.
///
/// Cheap to clone where you need to share it; holds a pooled connection agent
/// so repeated calls reuse TLS connections.
#[derive(Clone)]
pub struct Anthropic {
    pub(crate) agent: ureq::Agent,
    pub(crate) api_key: String,
    pub(crate) base_url: String,
    pub(crate) version: String,
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
        }
    }

    /// Override the base URL (e.g. to point at a proxy or gateway).
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Override the `anthropic-version` header.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    /// Start building a message request against the given model.
    pub fn message(&self, model: impl Into<String>) -> MessageBuilder<'_> {
        MessageBuilder::new(self, model.into())
    }
}

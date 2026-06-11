use crate::event::{ContentBlock, Usage};
use serde::Deserialize;

/// A completed (non-streaming) message from the API.
///
/// The full content is in [`content`](Self::content) as typed blocks; for the
/// common "just give me the text" case, use [`text`](Self::text).
#[derive(Debug, Clone, Deserialize)]
pub struct Response {
    pub id: String,
    pub model: String,
    pub role: String,
    pub content: Vec<ContentBlock>,
    #[serde(default)]
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub stop_sequence: Option<String>,
    #[serde(default)]
    pub usage: Usage,
}

impl Response {
    /// Concatenate every text block into one string.
    ///
    /// Thinking, tool-use, and other non-text blocks are skipped. This is the
    /// 90%-case accessor; reach into [`content`](Self::content) for the rest.
    pub fn text(&self) -> String {
        self.content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    /// The model declined the request for safety reasons.
    pub fn is_refusal(&self) -> bool {
        self.stop_reason.as_deref() == Some("refusal")
    }
}

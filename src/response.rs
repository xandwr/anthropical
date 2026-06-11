use crate::event::{ContentBlock, StopDetails, Usage};
use crate::request::Msg;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A complete message from the API: the body of a non-streaming response, the
/// result of [`EventStream::collect_message`](crate::EventStream::collect_message),
/// and the skeleton carried by a `message_start` event.
///
/// The full content is in [`content`](Self::content) as typed blocks; for the
/// common "just give me the text" case, use [`text`](Self::text).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub content: Vec<ContentBlock>,
    #[serde(default)]
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub stop_sequence: Option<String>,
    #[serde(default)]
    pub stop_details: Option<StopDetails>,
    #[serde(default)]
    pub usage: Usage,
    /// The `request-id` response header; quote it when reporting API issues.
    #[serde(skip)]
    pub request_id: Option<String>,
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

    /// Every `tool_use` block as `(id, name, input)`, ready to dispatch.
    pub fn tool_uses(&self) -> impl Iterator<Item = (&str, &str, &Value)> {
        self.content.iter().filter_map(|block| match block {
            ContentBlock::ToolUse { id, name, input } => {
                Some((id.as_str(), name.as_str(), input))
            }
            _ => None,
        })
    }

    /// The model declined the request for safety reasons.
    ///
    /// [`stop_details`](Self::stop_details) carries the category and
    /// explanation when the API provides them.
    pub fn is_refusal(&self) -> bool {
        self.stop_reason.as_deref() == Some("refusal")
    }

    /// Re-package this message as an assistant turn, for echoing back to the
    /// API in a tool-use loop. Thinking signatures and unknown block types
    /// are preserved verbatim.
    pub fn to_msg(&self) -> Msg {
        Msg::assistant_blocks(self.content.iter().map(ContentBlock::to_value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn to_msg_round_trips_content_losslessly() {
        let body = json!({
            "id": "msg_1",
            "model": "m",
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "hmm", "signature": "sig_1"},
                {"type": "text", "text": "calling a tool"},
                {"type": "tool_use", "id": "tu_1", "name": "lookup", "input": {"q": "x"}},
                {"type": "some_future_block", "payload": {"deep": [1, 2]}}
            ]
        });
        let response: Response = serde_json::from_value(body.clone()).unwrap();
        let echoed = serde_json::to_value(response.to_msg()).unwrap();
        assert_eq!(echoed["role"], "assistant");
        assert_eq!(echoed["content"], body["content"]);
    }

    #[test]
    fn text_and_tool_uses_accessors() {
        let response: Response = serde_json::from_value(json!({
            "id": "msg_1",
            "content": [
                {"type": "text", "text": "a"},
                {"type": "tool_use", "id": "tu_1", "name": "f", "input": {"k": 1}},
                {"type": "text", "text": "b"}
            ],
            "stop_reason": "refusal"
        }))
        .unwrap();
        assert_eq!(response.text(), "ab");
        assert!(response.is_refusal());
        let tools: Vec<_> = response.tool_uses().collect();
        assert_eq!(tools, vec![("tu_1", "f", &json!({"k": 1}))]);
    }
}

use crate::response::Response;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid event JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid UTF-8 in event stream: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("malformed event: {0}")]
    Malformed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// Opens a message; carries the skeleton the rest of the stream fills in.
    MessageStart {
        message: Response,
    },
    ContentBlockStart {
        index: u32,
        content_block: ContentBlock,
    },
    ContentBlockDelta {
        index: u32,
        delta: Delta,
    },
    ContentBlockStop {
        index: u32,
    },
    MessageDelta {
        delta: MessageDelta,
        #[serde(default)]
        usage: Usage,
    },
    MessageStop,
    Ping,
    Error {
        error: Value,
    },
    #[serde(other)]
    Unknown,
}

/// One block of message content.
///
/// Block types the crate doesn't model are preserved verbatim in
/// [`Other`](Self::Other), so a response can be echoed back to the API
/// (see [`Response::to_msg`]) without losing anything.
#[derive(Debug, Clone)]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    Thinking {
        thinking: String,
        /// Opaque integrity signature; must be passed back unmodified when
        /// echoing the block in a multi-turn conversation.
        signature: String,
    },
    ServerToolUse {
        id: String,
        name: String,
        input: Value,
    },
    Other(Value),
}

impl ContentBlock {
    /// The wire representation of this block.
    pub fn to_value(&self) -> Value {
        match self {
            Self::Text { text } => json!({"type": "text", "text": text}),
            Self::ToolUse { id, name, input } => {
                json!({"type": "tool_use", "id": id, "name": name, "input": input})
            }
            Self::Thinking {
                thinking,
                signature,
            } => json!({"type": "thinking", "thinking": thinking, "signature": signature}),
            Self::ServerToolUse { id, name, input } => {
                json!({"type": "server_tool_use", "id": id, "name": name, "input": input})
            }
            Self::Other(v) => v.clone(),
        }
    }

    fn from_value(v: Value) -> Self {
        fn s(v: &Value, key: &str) -> String {
            v.get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        }
        fn val(v: &Value, key: &str) -> Value {
            v.get(key).cloned().unwrap_or(Value::Null)
        }
        match v.get("type").and_then(Value::as_str) {
            Some("text") => Self::Text { text: s(&v, "text") },
            Some("tool_use") => Self::ToolUse {
                id: s(&v, "id"),
                name: s(&v, "name"),
                input: val(&v, "input"),
            },
            Some("thinking") => Self::Thinking {
                thinking: s(&v, "thinking"),
                signature: s(&v, "signature"),
            },
            Some("server_tool_use") => Self::ServerToolUse {
                id: s(&v, "id"),
                name: s(&v, "name"),
                input: val(&v, "input"),
            },
            _ => Self::Other(v),
        }
    }
}

impl Serialize for ContentBlock {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_value().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ContentBlock {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_value(Value::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Delta {
    TextDelta {
        text: String,
    },
    InputJsonDelta {
        partial_json: String,
    },
    ThinkingDelta {
        thinking: String,
    },
    SignatureDelta {
        signature: String,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessageDelta {
    #[serde(default)]
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub stop_sequence: Option<String>,
    #[serde(default)]
    pub stop_details: Option<StopDetails>,
}

/// Structured detail accompanying a `refusal` stop reason.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StopDetails {
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub explanation: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub cache_read_input_tokens: Option<u64>,
    #[serde(default)]
    pub cache_creation_input_tokens: Option<u64>,
}

impl Usage {
    /// Fold a later usage report in, keeping the freshest value per field.
    pub(crate) fn merge(&mut self, other: &Usage) {
        let fields = [
            (&mut self.input_tokens, other.input_tokens),
            (&mut self.output_tokens, other.output_tokens),
            (&mut self.cache_read_input_tokens, other.cache_read_input_tokens),
            (
                &mut self.cache_creation_input_tokens,
                other.cache_creation_input_tokens,
            ),
        ];
        for (mine, theirs) in fields {
            if theirs.is_some() {
                *mine = theirs;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_delta_tolerates_missing_usage_and_carries_stop_details() {
        let event: Event = serde_json::from_str(
            r#"{"type":"message_delta","delta":{"stop_reason":"refusal","stop_details":{"category":"cyber","explanation":"no"}}}"#,
        )
        .unwrap();
        match event {
            Event::MessageDelta { delta, usage } => {
                assert_eq!(delta.stop_reason.as_deref(), Some("refusal"));
                let details = delta.stop_details.unwrap();
                assert_eq!(details.category.as_deref(), Some("cyber"));
                assert!(usage.output_tokens.is_none());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn unknown_block_round_trips_verbatim() {
        let raw = json!({"type": "redacted_thinking", "data": "opaque-bytes"});
        let block: ContentBlock = serde_json::from_value(raw.clone()).unwrap();
        assert!(matches!(block, ContentBlock::Other(_)));
        assert_eq!(block.to_value(), raw);
    }

    #[test]
    fn thinking_block_keeps_its_signature() {
        let raw = json!({"type": "thinking", "thinking": "hmm", "signature": "sig_1"});
        let block: ContentBlock = serde_json::from_value(raw.clone()).unwrap();
        assert_eq!(block.to_value(), raw);
    }
}

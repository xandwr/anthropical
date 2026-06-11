use crate::error::{Error, Result};
use crate::event::{ContentBlock, Delta, Event, ParseError};
use crate::parser::EventParser;
use crate::response::Response;
use std::collections::HashMap;
use std::io::Read;

/// A live stream of [`Event`]s from a streaming request.
///
/// Pull events with [`recv`](Self::recv): it reads from the wire as needed,
/// feeding bytes through the [`EventParser`] until a full event is available.
/// Returns `Ok(None)` when the stream is exhausted. A mid-stream `error`
/// event (e.g. `overloaded_error`) surfaces as [`Error::Api`] rather than as
/// an event, so it can't be silently skipped.
///
/// To skip event handling entirely and just get the finished message, use
/// [`collect_message`](Self::collect_message).
pub struct EventStream {
    reader: Box<dyn Read + Send + Sync>,
    parser: EventParser,
    chunk: [u8; 8192],
    done: bool,
    request_id: Option<String>,
}

impl EventStream {
    pub(crate) fn new(
        reader: impl Read + Send + Sync + 'static,
        request_id: Option<String>,
    ) -> Self {
        Self {
            reader: Box::new(reader),
            parser: EventParser::new(),
            chunk: [0u8; 8192],
            done: false,
            request_id,
        }
    }

    /// The `request-id` header from the streaming response.
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// Pull the next event, reading more from the wire if the parser is empty.
    ///
    /// Returns `Ok(None)` once the stream is exhausted. For `for`-loop or
    /// iterator-adapter use, this type also implements [`Iterator`].
    pub fn recv(&mut self) -> Result<Option<Event>> {
        loop {
            if let Some(event) = self.parser.next_event() {
                if let Event::Error { error } = &event {
                    return Err(Error::api_event(error));
                }
                return Ok(Some(event));
            }
            if self.done {
                return Ok(None);
            }
            let n = self.reader.read(&mut self.chunk)?;
            if n == 0 {
                self.done = true;
                // Flush any final buffered event before reporting end-of-stream.
                continue;
            }
            self.parser.feed(&self.chunk[..n])?;
        }
    }

    /// Drain the stream and assemble the complete message: the streaming
    /// equivalent of [`send`](crate::MessageBuilder::send).
    ///
    /// Text, thinking, and tool-input deltas are folded into their blocks
    /// (tool `input` is parsed from the accumulated JSON fragments), and the
    /// final stop reason and usage are merged in. Errors if the stream ends
    /// before `message_stop`, so a dropped connection can't masquerade as a
    /// complete response.
    pub fn collect_message(mut self) -> Result<Response> {
        fn started(m: &mut Option<Response>) -> Result<&mut Response> {
            m.as_mut()
                .ok_or_else(|| malformed("event before message_start"))
        }

        let mut message: Option<Response> = None;
        let mut tool_json: HashMap<usize, String> = HashMap::new();
        let mut stopped = false;

        while let Some(event) = self.recv()? {
            match event {
                Event::MessageStart { message: m } => message = Some(m),
                Event::ContentBlockStart { content_block, .. } => {
                    started(&mut message)?.content.push(content_block);
                }
                Event::ContentBlockDelta { index, delta } => {
                    let index = index as usize;
                    let msg = started(&mut message)?;
                    let Some(block) = msg.content.get_mut(index) else {
                        return Err(malformed("delta for a block that never started"));
                    };
                    match (block, delta) {
                        (ContentBlock::Text { text }, Delta::TextDelta { text: t }) => {
                            text.push_str(&t)
                        }
                        (
                            ContentBlock::Thinking { thinking, .. },
                            Delta::ThinkingDelta { thinking: t },
                        ) => thinking.push_str(&t),
                        (
                            ContentBlock::Thinking { signature, .. },
                            Delta::SignatureDelta { signature: s },
                        ) => *signature = s,
                        (_, Delta::InputJsonDelta { partial_json }) => {
                            tool_json.entry(index).or_default().push_str(&partial_json)
                        }
                        _ => {}
                    }
                }
                Event::ContentBlockStop { index } => {
                    let index = index as usize;
                    if let Some(json) = tool_json.remove(&index)
                        && !json.is_empty()
                    {
                        let input_value = serde_json::from_str(&json)?;
                        if let Some(
                            ContentBlock::ToolUse { input, .. }
                            | ContentBlock::ServerToolUse { input, .. },
                        ) = started(&mut message)?.content.get_mut(index)
                        {
                            *input = input_value;
                        }
                    }
                }
                Event::MessageDelta { delta, usage } => {
                    let msg = started(&mut message)?;
                    if delta.stop_reason.is_some() {
                        msg.stop_reason = delta.stop_reason;
                    }
                    if delta.stop_sequence.is_some() {
                        msg.stop_sequence = delta.stop_sequence;
                    }
                    if delta.stop_details.is_some() {
                        msg.stop_details = delta.stop_details;
                    }
                    msg.usage.merge(&usage);
                }
                Event::MessageStop => stopped = true,
                Event::Ping | Event::Unknown | Event::Error { .. } => {}
            }
        }

        let mut msg = message.ok_or_else(|| malformed("stream ended before message_start"))?;
        if !stopped {
            return Err(malformed("stream ended before message_stop"));
        }
        msg.request_id = self.request_id;
        Ok(msg)
    }
}

fn malformed(why: &str) -> Error {
    Error::Stream(ParseError::Malformed(why.into()))
}

impl Iterator for EventStream {
    type Item = Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.recv() {
            Ok(Some(event)) => Some(Ok(event)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::VecDeque;

    struct Script(VecDeque<Vec<u8>>);

    impl Read for Script {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            match self.0.pop_front() {
                Some(chunk) => {
                    buf[..chunk.len()].copy_from_slice(&chunk);
                    Ok(chunk.len())
                }
                None => Ok(0),
            }
        }
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("connection reset"))
        }
    }

    fn sse(records: &[Value]) -> Vec<u8> {
        records
            .iter()
            .map(|r| format!("data: {r}\n\n"))
            .collect::<String>()
            .into_bytes()
    }

    /// Deliver the bytes in hostile little chunks to exercise reassembly.
    fn stream_of(records: &[Value]) -> EventStream {
        let chunks = sse(records).chunks(7).map(<[u8]>::to_vec).collect();
        EventStream::new(Script(chunks), Some("req_test".into()))
    }

    fn full_stream() -> Vec<Value> {
        vec![
            json!({"type": "message_start", "message": {
                "id": "msg_1", "role": "assistant", "model": "claude-opus-4-8",
                "content": [], "usage": {"input_tokens": 10}
            }}),
            json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Hello"}}),
            json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": " world"}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "content_block_start", "index": 1, "content_block": {"type": "tool_use", "id": "tu_1", "name": "get_weather", "input": {}}}),
            json!({"type": "content_block_delta", "index": 1, "delta": {"type": "input_json_delta", "partial_json": "{\"city\":"}}),
            json!({"type": "content_block_delta", "index": 1, "delta": {"type": "input_json_delta", "partial_json": "\"Paris\"}"}}),
            json!({"type": "content_block_stop", "index": 1}),
            json!({"type": "content_block_start", "index": 2, "content_block": {"type": "thinking", "thinking": ""}}),
            json!({"type": "content_block_delta", "index": 2, "delta": {"type": "thinking_delta", "thinking": "hmm"}}),
            json!({"type": "content_block_delta", "index": 2, "delta": {"type": "signature_delta", "signature": "sig_abc"}}),
            json!({"type": "content_block_stop", "index": 2}),
            json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 7}}),
            json!({"type": "message_stop"}),
        ]
    }

    #[test]
    fn collect_message_assembles_everything() {
        let msg = stream_of(&full_stream()).collect_message().unwrap();
        assert_eq!(msg.id, "msg_1");
        assert_eq!(msg.text(), "Hello world");
        assert_eq!(msg.stop_reason.as_deref(), Some("tool_use"));
        assert_eq!(msg.usage.input_tokens, Some(10));
        assert_eq!(msg.usage.output_tokens, Some(7));
        assert_eq!(msg.request_id.as_deref(), Some("req_test"));

        let tools: Vec<_> = msg.tool_uses().collect();
        assert_eq!(tools, vec![("tu_1", "get_weather", &json!({"city": "Paris"}))]);

        match &msg.content[2] {
            ContentBlock::Thinking {
                thinking,
                signature,
            } => {
                assert_eq!(thinking, "hmm");
                assert_eq!(signature, "sig_abc");
            }
            other => panic!("unexpected block: {other:?}"),
        }
    }

    #[test]
    fn truncated_stream_is_an_error_not_a_partial_message() {
        let mut records = full_stream();
        records.pop(); // drop message_stop
        let err = stream_of(&records).collect_message().unwrap_err();
        assert!(matches!(err, Error::Stream(ParseError::Malformed(_))));
    }

    #[test]
    fn mid_stream_error_event_surfaces_on_the_error_path() {
        let records = vec![
            full_stream()[0].clone(),
            json!({"type": "error", "error": {"type": "overloaded_error", "message": "busy"}}),
        ];
        let mut stream = stream_of(&records);
        assert!(matches!(stream.recv(), Ok(Some(Event::MessageStart { .. }))));
        match stream.recv() {
            Err(Error::Api { status, kind, .. }) => {
                assert_eq!(status, None);
                assert_eq!(kind, "overloaded_error");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn reader_failure_propagates_as_io_error() {
        let mut stream = EventStream::new(FailingReader, None);
        assert!(matches!(stream.recv(), Err(Error::Io(_))));
    }

    #[test]
    fn iterator_yields_events_then_ends() {
        let events: Vec<_> = stream_of(&full_stream()).collect::<Result<_>>().unwrap();
        assert_eq!(events.len(), 15);
        assert!(matches!(events.last(), Some(Event::MessageStop)));
    }
}

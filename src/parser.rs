use crate::event::{Event, ParseError};

/// Incremental parser for the Messages API server-sent event stream.
///
/// Feed it raw bytes as they arrive off the wire; it buffers until it has a
/// complete SSE record (delimited by a blank line) and yields one [`Event`]
/// per record. Bytes are buffered, not decoded eagerly, so a multi-byte UTF-8
/// character split across two reads is reassembled correctly.
pub struct EventParser {
    buf: Vec<u8>,
    pending: std::collections::VecDeque<Event>,
}

impl EventParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            pending: std::collections::VecDeque::new(),
        }
    }

    /// Append a chunk and parse any records it completes.
    ///
    /// Returns the number of events queued. A record whose `data` fails to
    /// deserialize surfaces as [`ParseError`] rather than being silently
    /// dropped; the buffer is still advanced past it so the stream recovers.
    pub fn feed(&mut self, bytes: &[u8]) -> Result<usize, ParseError> {
        self.buf.extend_from_slice(bytes);
        let mut new_events = 0;
        while let Some(idx) = find_record_end(&self.buf) {
            let record: Vec<u8> = self.buf.drain(..idx).collect();
            if let Some(event) = parse_record(&record)? {
                self.pending.push_back(event);
                new_events += 1;
            }
        }
        Ok(new_events)
    }

    pub fn next_event(&mut self) -> Option<Event> {
        self.pending.pop_front()
    }

    pub fn drain(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.pending).into_iter().collect()
    }

    /// Parse a complete in-memory stream in one shot.
    pub fn parse_all(text: &str) -> Result<Vec<Event>, ParseError> {
        let mut p = Self::new();
        p.feed(text.as_bytes())?;
        Ok(p.drain())
    }
}

impl Default for EventParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the byte index just past the record-terminating blank line
/// (`\n\n` or `\r\n\r\n`), or `None` if no complete record is buffered yet.
fn find_record_end(buf: &[u8]) -> Option<usize> {
    buf.windows(2)
        .position(|w| w == b"\n\n")
        .map(|i| i + 2)
        .or_else(|| buf.windows(4).position(|w| w == b"\r\n\r\n").map(|i| i + 4))
}

/// Decode one SSE record and deserialize its concatenated `data:` lines.
/// Returns `Ok(None)` for records carrying no data (e.g. a lone comment).
fn parse_record(record: &[u8]) -> Result<Option<Event>, ParseError> {
    let text = std::str::from_utf8(record)?;
    let mut data = String::new();
    for line in text.split('\n') {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if data.is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_str(&data)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{ContentBlock, Delta, Event};

    #[test]
    fn parses_a_full_stream() {
        let stream = "\
event: message_start\n\
data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"role\":\"assistant\"}}\n\
\n\
event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\
\n\
event: message_stop\n\
data: {\"type\":\"message_stop\"}\n\
\n";
        let events = EventParser::parse_all(stream).unwrap();
        assert_eq!(events.len(), 3);
        assert!(matches!(events[0], Event::MessageStart { .. }));
        assert!(matches!(
            &events[1],
            Event::ContentBlockDelta { delta: Delta::TextDelta { text }, .. } if text == "hi"
        ));
        assert!(matches!(events[2], Event::MessageStop));
    }

    #[test]
    fn reassembles_multibyte_char_split_across_feeds() {
        // "🦀" is 4 bytes (F0 9F A6 80); cut the record between bytes 2 and 3.
        let record = "event: content_block_delta\n\
data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"🦀\"}}\n\n";
        let bytes = record.as_bytes();
        let split = bytes.iter().position(|&b| b == 0xF0).unwrap() + 2;

        let mut p = EventParser::new();
        assert_eq!(p.feed(&bytes[..split]).unwrap(), 0); // incomplete record, no panic
        assert_eq!(p.feed(&bytes[split..]).unwrap(), 1);

        let ev = p.next_event().unwrap();
        assert!(matches!(
            ev,
            Event::ContentBlockDelta { delta: Delta::TextDelta { text }, .. } if text == "🦀"
        ));
    }

    #[test]
    fn malformed_data_surfaces_as_error() {
        let mut p = EventParser::new();
        let err = p.feed(b"data: {not valid json}\n\n").unwrap_err();
        assert!(matches!(err, ParseError::Json(_)));
    }

    #[test]
    fn unknown_block_type_falls_back_gracefully() {
        let stream = "data: {\"type\":\"content_block_start\",\"index\":0,\
\"content_block\":{\"type\":\"some_future_block\",\"data\":\"x\"}}\n\n";
        let events = EventParser::parse_all(stream).unwrap();
        assert!(matches!(
            events[0],
            Event::ContentBlockStart {
                content_block: ContentBlock::Other,
                ..
            }
        ));
    }
}

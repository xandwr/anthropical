use crate::event::{Event, ParseError};

pub struct EventParser {
    buf: String,
    pending: std::collections::VecDeque<Event>,
}

impl EventParser {
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            pending: std::collections::VecDeque::new(),
        }
    }

    pub fn feed(&mut self, bytes: &[u8]) -> usize {
        self.buf.push_str(&String::from_utf8_lossy(bytes));
        let mut new_events = 0;
        while let Some(idx) = self.buf.find("\n\n") {
            let raw = self.buf[..idx].to_string();
            self.buf.drain(..idx + 2);
            if let Some(event) = parse_event_block(&raw) {
                self.pending.push_back(event);
                new_events += 1;
            }
        }
        new_events
    }

    pub fn next_event(&mut self) -> Result<Option<Event>, ParseError> {
        Ok(self.pending.pop_front())
    }

    pub fn drain(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.pending).into_iter().collect()
    }

    pub fn parse_all(text: &str) -> Vec<Event> {
        let mut p = Self::new();
        p.feed(text.as_bytes());
        p.drain()
    }
}

impl Default for EventParser {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_event_block(raw: &str) -> Option<Event> {
    let mut data = String::new();
    for line in raw.split('\n') {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if data.is_empty() {
        return None;
    }
    serde_json::from_str(&data).ok()
}

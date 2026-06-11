use crate::error::Result;
use crate::event::Event;
use crate::parser::EventParser;
use std::io::Read;

/// A live stream of [`Event`]s from a streaming request.
///
/// Pull events with [`next`](Self::next): it reads from the wire as needed,
/// feeding bytes through the [`EventParser`] until a full event is available.
/// Returns `Ok(None)` when the stream is exhausted.
pub struct EventStream {
    reader: Box<dyn Read + Send + Sync>,
    parser: EventParser,
    chunk: [u8; 8192],
    done: bool,
}

impl EventStream {
    pub(crate) fn new(reader: impl Read + Send + Sync + 'static, parser: EventParser) -> Self {
        Self {
            reader: Box::new(reader),
            parser,
            chunk: [0u8; 8192],
            done: false,
        }
    }

    /// Pull the next event, reading more from the wire if the parser is empty.
    ///
    /// Returns `Ok(None)` once the stream is exhausted. For `for`-loop or
    /// iterator-adapter use, this type also implements [`Iterator`].
    pub fn recv(&mut self) -> Result<Option<Event>> {
        loop {
            if let Some(event) = self.parser.next_event() {
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

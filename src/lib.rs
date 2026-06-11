//! A lightweight, blocking client for the Anthropic Messages API.

mod client;
mod error;
mod event;
mod parser;
mod request;
mod response;
mod stream;

pub use crate::client::Anthropic;
pub use crate::error::{Error, Result};
pub use crate::event::{
    ContentBlock, Delta, Event, MessageDelta, ParseError, StopDetails, Usage,
};
pub use crate::parser::EventParser;
pub use crate::request::{Content, MessageBuilder, Msg, tool_error, tool_result};
pub use crate::response::Response;
pub use crate::stream::EventStream;

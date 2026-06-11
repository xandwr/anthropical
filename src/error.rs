use crate::event::ParseError;
use thiserror::Error;

/// Anything that can go wrong talking to the API.
#[derive(Debug, Error)]
pub enum Error {
    /// The API returned a non-2xx status with a structured error body.
    #[error("api error {status} ({kind}): {message}")]
    Api {
        status: u16,
        /// The API's `error.type`, e.g. `"invalid_request_error"`.
        kind: String,
        message: String,
    },

    /// Transport failure (connection, TLS, timeout) before a response arrived.
    #[error("transport error: {0}")]
    Transport(#[from] ureq::Error),

    /// A response body we couldn't decode into the expected shape.
    #[error("decode error: {0}")]
    Decode(#[from] serde_json::Error),

    /// A failure while parsing the streaming event feed.
    #[error(transparent)]
    Stream(#[from] ParseError),

    /// I/O failure while reading a (streaming) response body.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

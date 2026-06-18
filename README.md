# anthropical

A small, pure-Rust client for the [Anthropic Messages API](https://docs.anthropic.com/en/api/messages).

It is blocking (no async runtime required), has a tiny dependency tree, and leans on serde for everything on the wire. The goal is a library you can drop into a Rust project to talk to Claude without pulling in tokio, reqwest, or a large SDK.

> **Status: early and pre-release.** The crate is at `0.0.1` and is not yet published to crates.io (`publish = false` in `Cargo.toml`). The API surface described below works and is covered by tests, but names and signatures may still change.

## What's in the box

The crate is currently one library with no binaries. The public API (re-exported from [`src/lib.rs`](src/lib.rs)) is:

| Area | Type / function | What it does |
| --- | --- | --- |
| Client | `Anthropic` | A cheap-to-clone handle holding a pooled connection agent, API key, base URL, version, timeouts, and retry count. |
| Request | `MessageBuilder` | A fluent builder for a Messages request: model, `max_tokens`, `system`, `temperature`, message turns, extra fields, and headers. |
| Sending | `MessageBuilder::send` | Blocking call that returns the full `Response`. |
| Streaming | `MessageBuilder::stream` | Returns an `EventStream` of server-sent events as they arrive. |
| Counting | `MessageBuilder::count_tokens` | Returns the input token count for a request without running it. |
| Response | `Response` | A complete message, with `text()`, `tool_uses()`, `is_refusal()`, and `to_msg()` helpers. |
| Messages | `Msg`, `Content` | Conversation turns as plain text or raw content blocks. |
| Tool use | `tool_result`, `tool_error` | Build `tool_result` content blocks to feed back into a tool-use loop. |
| Streaming internals | `EventStream`, `EventParser`, `Event`, `Delta`, `ContentBlock`, `Usage`, ... | Incremental SSE parsing and event types. |
| Errors | `Error`, `Result`, `ParseError` | Typed errors for API, transport, decode, stream, and I/O failures. |

The source is split by responsibility so each piece is easy to find and edit:

| File | Responsibility |
| --- | --- |
| [`src/client.rs`](src/client.rs) | The `Anthropic` client and its configuration setters. |
| [`src/request.rs`](src/request.rs) | `MessageBuilder`, message types, the HTTP POST path, and retry logic. |
| [`src/response.rs`](src/response.rs) | The `Response` type and its accessors. |
| [`src/event.rs`](src/event.rs) | Streaming event, delta, content-block, and usage types. |
| [`src/parser.rs`](src/parser.rs) | `EventParser`, the incremental SSE record parser. |
| [`src/stream.rs`](src/stream.rs) | `EventStream`, which drives the parser off the wire. |
| [`src/error.rs`](src/error.rs) | The `Error` enum and error mapping. |
| [`src/lib.rs`](src/lib.rs) | Module wiring and public re-exports. |
| [`tests/integration.rs`](tests/integration.rs) | End-to-end tests against a local mock HTTP server. |

## What it can do today

- **Send a message and get the reply**, with the response body decoded into typed content blocks.
- **Stream a response** as server-sent events, either event by event or collected into a final message.
- **Count input tokens** for a request without generating a completion.
- **Tool use:** read `tool_use` blocks off a response, send `tool_result` / `tool_error` blocks back, and echo assistant turns verbatim (thinking signatures and unknown block types are preserved, so nothing is lost across a turn).
- **Retries** on connection failures, `408`, `429`, and `5xx`, with exponential backoff that honors a server `retry-after`.
- **Typed errors** that distinguish API errors (with status, error type, message, request id, and retry-after) from transport, decode, stream, and I/O failures.
- **Escape hatches** for anything without a dedicated setter: `with(key, value)` adds an arbitrary top-level request field, and `header(name, value)` adds a request header (for example `anthropic-beta`).

## Usage

Add it as a git or path dependency for now (it is not on crates.io yet):

```toml
[dependencies]
anthropical = { git = "https://github.com/xandwr/anthropical" }
```

### Send a message

```rust
use anthropical::Anthropic;

let client = Anthropic::from_env()?; // reads ANTHROPIC_API_KEY
let response = client
    .message("claude-opus-4-8")
    .system("You are concise.")
    .user("Say hello in one word.")
    .max_tokens(64)
    .send()?;

println!("{}", response.text());
```

### Stream a response

```rust
use anthropical::Anthropic;

let stream = Anthropic::from_env()?
    .message("claude-opus-4-8")
    .user("Count to five.")
    .stream()?;

// Either drive events yourself...
for event in stream {
    let event = event?;
    // match on `event` here
}

// ...or collect the finished message in one call:
// let message = stream.collect_message()?;
```

### Count tokens

```rust
let tokens = Anthropic::from_env()?
    .message("claude-opus-4-8")
    .user("How many tokens is this?")
    .count_tokens()?;
```

## Configuration

`Anthropic::new(api_key)` gives sensible defaults. `Anthropic::from_env()` reads `ANTHROPIC_API_KEY` and honors `ANTHROPIC_BASE_URL` when set. Every setter returns the client so they chain:

```rust
use std::time::Duration;
use anthropical::Anthropic;

let client = Anthropic::new(api_key)
    .base_url("https://my-gateway.example.com") // point at a proxy or gateway
    .version("2023-06-01")                       // anthropic-version header
    .timeout(Duration::from_secs(600))           // response deadline
    .connect_timeout(Duration::from_secs(10))    // TCP connect deadline
    .max_retries(2);
```

## Async note

All calls block the calling thread. In an async program, run them with something like `tokio::task::spawn_blocking` or on a dedicated thread.

## Dependencies

Kept deliberately small:

- [`ureq`](https://crates.io/crates/ureq) (rustls, no default features) for HTTP
- [`serde`](https://crates.io/crates/serde) and [`serde_json`](https://crates.io/crates/serde_json) for the wire format
- [`thiserror`](https://crates.io/crates/thiserror) for the error type

Requires Rust edition 2024 (`rust-version = 1.96.0`).

## Building and testing

```sh
cargo build
cargo test
```

The integration tests in [`tests/integration.rs`](tests/integration.rs) spin up a local mock HTTP server, so they run offline and need no API key.

## Roadmap and contributing

This is an early personal project, so expect rough edges and breaking changes. Contributions, issues, and suggestions are welcome at <https://github.com/xandwr/anthropical>.

Likely next steps include typed setters for more request parameters (tools, thinking, `top_p`), broader content-block coverage, and a path to publishing on crates.io.

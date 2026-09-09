//! HTTP sync transport implementation.
//!
//! [`HttpSyncTransport`] implements [`SyncTransport`] using `reqwest` for
//! HTTP communication. Uses postcard serialization for compact payloads.
//!
//! Every body — handshake, push and pull, request and reply — carries the
//! serializer-independent wire frame header, validated by raw byte-slice before
//! any deserialize, so a cross-version peer fails loud in BOTH directions and a
//! misrouted body is refused before the decoder sees it.
//!
//! Response bodies are capped (default [`DEFAULT_MAX_REQUEST_BYTES`], the same
//! cap the server applies to requests): a `Content-Length` above the cap is
//! refused without reading the body, and a body without one is read bounded
//! and refused the moment it crosses the cap — either way as the typed
//! [`SyncError::PayloadTooLarge`], before any postcard decode.
//!
//! That cap is **inbound only**. What this client will read says nothing about
//! what a peer will accept, so an outbound request is encoded against the
//! `send_budget_bytes` its caller supplies — the cap its packer sized against —
//! and never against the response reader. The two are legitimately unequal.
//!
//! # Example
//!
//! ```rust,ignore
//! use pulsedb::sync::transport_http::HttpSyncTransport;
//!
//! let transport = HttpSyncTransport::new("http://server:3000");
//! // or with authentication:
//! let transport = HttpSyncTransport::with_auth("https://server:3000", "my-secret-token");
//! // or with a tighter response-body cap:
//! let transport = HttpSyncTransport::new("http://server:3000").with_max_response_bytes(4 * 1024 * 1024);
//! ```

use async_trait::async_trait;
use reqwest::{Client, Response};
use tracing::{debug, warn};

use super::config::DEFAULT_MAX_REQUEST_BYTES;
use super::error::SyncError;
use super::transport::SyncTransport;
use super::types::{
    HandshakeRequest, HandshakeResponse, PullPage, PullRequest, PushAck, PushRequest, WireReply,
};
use super::wire::{self, WireOperation};

/// HTTP-based sync transport using reqwest.
///
/// Communicates with a remote PulseDB sync server over HTTP using framed,
/// postcard-serialized request/response bodies.
///
/// # Endpoints
///
/// | Method | Path | Request | Response |
/// |--------|------|---------|----------|
/// | POST | `/sync/handshake` | `HandshakeRequest` | `HandshakeResponse` |
/// | POST | `/sync/push` | `PushRequest` | `WireReply<PushAck>` |
/// | POST | `/sync/pull` | `PullRequest` | `WireReply<PullPage>` |
/// | GET | `/sync/health` | (none) | 200 OK |
///
/// Every request and response body is a framed message (see
/// [`wire`](super::wire)); `/sync/health` carries no body at all and is
/// liveness only.
pub struct HttpSyncTransport {
    client: Client,
    base_url: String,
    auth_token: Option<String>,
    max_response_bytes: usize,
}

impl HttpSyncTransport {
    /// Creates a new HTTP transport pointing at the given base URL.
    ///
    /// The URL should not include a trailing slash.
    /// Example: `"http://localhost:3000"` or `"https://api.example.com"`
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into(),
            auth_token: None,
            max_response_bytes: DEFAULT_MAX_REQUEST_BYTES,
        }
    }

    /// Creates a new HTTP transport with Bearer token authentication.
    ///
    /// The token is sent as `Authorization: Bearer {token}` on every request.
    pub fn with_auth(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into(),
            auth_token: Some(token.into()),
            max_response_bytes: DEFAULT_MAX_REQUEST_BYTES,
        }
    }

    /// Sets the **response**-body byte cap (default
    /// [`DEFAULT_MAX_REQUEST_BYTES`]).
    ///
    /// Inbound only: it bounds what this client reads, never what it sends. A
    /// client that reads 4 MiB may still send a 64 MiB request to a peer that
    /// accepts one — the outbound budget arrives per call.
    ///
    /// Mirrors the server's `SyncConfig::max_request_bytes` on the client
    /// side: a response whose `Content-Length` exceeds the cap is refused
    /// without reading the body, and a response without a `Content-Length` is
    /// read bounded and refused once it crosses the cap. Both surface as the
    /// typed [`SyncError::PayloadTooLarge`] before any postcard decode.
    pub fn with_max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = max_response_bytes;
        self
    }

    /// Returns the response-body byte cap in force.
    pub fn max_response_bytes(&self) -> usize {
        self.max_response_bytes
    }

    /// Reads a response body under the byte cap.
    ///
    /// A declared `Content-Length` above the cap is refused before a single
    /// body byte is read; otherwise the body is accumulated chunk by chunk and
    /// refused as soon as it would exceed the cap. No streaming decode — the
    /// caller receives either the whole (in-cap) body or the typed error.
    async fn read_body_bounded(mut response: Response, max: usize) -> Result<Vec<u8>, SyncError> {
        if let Some(declared) = response.content_length() {
            if declared > max as u64 {
                let size = usize::try_from(declared).unwrap_or(usize::MAX);
                warn!(
                    size,
                    max_response_bytes = max,
                    "Refusing oversized sync response (Content-Length) before decode"
                );
                return Err(SyncError::PayloadTooLarge { size, max });
            }
        }

        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| SyncError::transport(format!("Failed to read response body: {}", e)))?
        {
            let size = body.len().saturating_add(chunk.len());
            if size > max {
                warn!(
                    size,
                    max_response_bytes = max,
                    "Refusing oversized sync response (bounded read) before decode"
                );
                return Err(SyncError::PayloadTooLarge { size, max });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    /// Sends a POST with a raw byte body and returns the raw response bytes.
    ///
    /// This is the framing-agnostic transport leg: it knows nothing about
    /// postcard or the frame header. [`post_framed`](Self::post_framed) layers
    /// framing and validation on top. The response body is read under
    /// [`Self::max_response_bytes`].
    async fn post_raw(&self, path: &str, body: Vec<u8>) -> Result<Vec<u8>, SyncError> {
        let url = format!("{}{}", self.base_url, path);

        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(body);

        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }

        let response = req.send().await.map_err(|e| {
            if e.is_timeout() {
                SyncError::Timeout
            } else if e.is_connect() {
                SyncError::ConnectionLost
            } else {
                SyncError::transport(e.to_string())
            }
        })?;

        let status = response.status();
        if !status.is_success() {
            // Error bodies are read under the same cap. An oversized one keeps
            // its typed identity — a caller checking `is_payload_too_large()`
            // must see it whether the cap was hit on a success or an error
            // response — while an ordinary unreadable body degrades to
            // "unknown" rather than masking the status.
            let body_text = match Self::read_body_bounded(response, self.max_response_bytes).await {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(err @ SyncError::PayloadTooLarge { .. }) => return Err(err),
                Err(_) => "unknown".into(),
            };
            return Err(if status.is_client_error() {
                SyncError::invalid_payload(format!("HTTP {}: {}", status, body_text))
            } else {
                SyncError::transport(format!("HTTP {}: {}", status, body_text))
            });
        }

        Self::read_body_bounded(response, self.max_response_bytes).await
    }

    /// Sends a POST with a framed body and decodes the framed response.
    ///
    /// Both legs go through [`wire`](super::wire), against **different** caps,
    /// because they are different directions. The request is refused before
    /// allocation if it exceeds `send_budget` — the caller's outbound budget,
    /// the same number its packer sized against. The response is checked
    /// against [`Self::max_response_bytes`], this client's actual reader, for
    /// cap, magic, wire version and operation before any decode.
    ///
    /// Encoding over budget is [`SyncError::RequestTooLarge`], not
    /// `PayloadTooLarge`: this side built a request too big for the budget it
    /// was given, which is deterministic and terminal, whereas an oversized
    /// body arriving from the wire is the sender's fault and stays
    /// `PayloadTooLarge`.
    async fn post_framed<Req, Resp>(
        &self,
        operation: WireOperation,
        path: &str,
        request: &Req,
        send_budget: usize,
    ) -> Result<Resp, SyncError>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let body = wire::encode_bounded(operation, request, send_budget)
            .map_err(|e| request_too_large(operation, e))?;
        let response_bytes = self.post_raw(path, body).await?;
        wire::decode_bounded(operation, &response_bytes, self.max_response_bytes)
    }
}

/// Reclassifies an **encode**-time over-budget refusal as
/// [`SyncError::RequestTooLarge`].
///
/// Only [`wire::encode_bounded`] feeds this, and only on the outbound leg, so
/// the size it reports is unambiguously "the request we built against the
/// budget we were given". Every other error passes through untouched — an
/// inbound `PayloadTooLarge` in particular, which is never reclassified.
fn request_too_large(operation: WireOperation, err: SyncError) -> SyncError {
    match err {
        SyncError::PayloadTooLarge { size, max } => SyncError::RequestTooLarge {
            operation,
            needed: size as u64,
            cap: max as u64,
        },
        other => other,
    }
}

#[async_trait]
impl SyncTransport for HttpSyncTransport {
    async fn handshake(
        &self,
        request: HandshakeRequest,
        send_budget_bytes: usize,
    ) -> Result<HandshakeResponse, SyncError> {
        debug!(url = %self.base_url, "HTTP sync handshake");
        self.post_framed(
            WireOperation::Handshake,
            "/sync/handshake",
            &request,
            send_budget_bytes,
        )
        .await
    }

    async fn push_changes(
        &self,
        request: PushRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<PushAck>, SyncError> {
        debug!(count = request.changes.len(), "HTTP sync push");
        self.post_framed(
            WireOperation::Push,
            "/sync/push",
            &request,
            send_budget_bytes,
        )
        .await
    }

    async fn pull_changes(
        &self,
        request: PullRequest,
        send_budget_bytes: usize,
    ) -> Result<WireReply<PullPage>, SyncError> {
        debug!("HTTP sync pull");
        self.post_framed(
            WireOperation::Pull,
            "/sync/pull",
            &request,
            send_budget_bytes,
        )
        .await
    }

    async fn health_check(&self) -> Result<(), SyncError> {
        let url = format!("{}/sync/health", self.base_url);

        let mut req = self.client.get(&url);
        if let Some(ref token) = self.auth_token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }

        let response = req.send().await.map_err(|e| {
            if e.is_timeout() {
                SyncError::Timeout
            } else if e.is_connect() {
                SyncError::ConnectionLost
            } else {
                SyncError::transport(e.to_string())
            }
        })?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(SyncError::transport(format!(
                "Health check failed: HTTP {}",
                response.status()
            )))
        }
    }

    /// The client's ACTUAL bounded-reader limit — the same number
    /// [`read_body_bounded`](Self::read_body_bounded) enforces, not a separate
    /// configured guess.
    fn receive_limit_bytes(&self) -> usize {
        self.max_response_bytes
    }
}

// HttpSyncTransport is Send + Sync (reqwest::Client is Send + Sync)

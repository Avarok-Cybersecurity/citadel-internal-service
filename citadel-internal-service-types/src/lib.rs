use citadel_internal_service_macros::{Cid, IsError, IsNotification, RequestId};
use citadel_types::crypto::PreSharedKey;
pub use citadel_types::prelude::{
    ConnectMode, MemberState, MessageGroupKey, ObjectId, ObjectTransferStatus, SecBuffer,
    SecurityLevel, SessionSecuritySettings, TransferType, UdpMode, UserIdentifier,
    VirtualObjectMetadata,
};
use custom_debug::Debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use uuid::Uuid;

#[cfg(feature = "typescript")]
use ts_rs::TS;

/// The `LocalDBGetKVFailure` message that means "no such key", as opposed to a
/// real backend error.
///
/// It is the pivot of a distinction the whole messaging queue rests on: a read
/// that found nothing is an empty map to be initialised, while a read that
/// FAILED must not be. Both sides live in different crates — the agent's
/// `local_db/get_kv.rs` writes it, the connector's `messenger/backend.rs`
/// compares against it — so it was a string literal typed out twice, matched
/// with `==`. A reworded message would not break a build or a test; it would
/// silently turn every genuine miss into a hard error, or a rename in the other
/// direction turn every error into "empty", which is the exact failure mode
/// that has already been fixed five times in the agent's request handlers.
pub const KEY_NOT_FOUND: &str = "Key not found";

/// Length only, for material that is the user's own content.
///
/// `bytes_debug_fmt` below shows the first and last five bytes, which is the right
/// trade for a key, a ratchet sample or a file chunk: it identifies the value
/// without disclosing anything a reader could use.
///
/// It is the wrong trade for a decrypted message BODY. The first five bytes of a
/// chat message are its opening word, and across a log they are a great many
/// opening words. The agent exists to keep this material off disk; a sample of it
/// in a log line is still the material.
///
/// The length is kept deliberately. It is what distinguishes an empty body from a
/// truncated one from a whole one, which is the question a delivery bug actually
/// asks, and it discloses nothing beyond what the ciphertext length already does.
pub fn plaintext_debug_fmt<T: AsRef<[u8]>>(
    val: &T,
    f: &mut std::fmt::Formatter,
) -> std::fmt::Result {
    write!(f, "{{Plaintext(len: {}, redacted)}}", val.as_ref().len())
}

pub fn bytes_debug_fmt<T: std::fmt::Debug + AsRef<[u8]>>(
    val: &T,
    f: &mut std::fmt::Formatter,
) -> std::fmt::Result {
    const SAMPLE_ENDS_COUNT: usize = 5;
    let slice = val.as_ref();
    let len = slice.len();
    if len <= (SAMPLE_ENDS_COUNT * 2) {
        return write!(f, "{{BytesLike(len: {len}. values: {slice:?})}}");
    }

    // Get the first and last 5 bytes
    let sample_ending_boundary = len.saturating_sub(SAMPLE_ENDS_COUNT);
    let first_bytes: &[u8] = &slice[..SAMPLE_ENDS_COUNT];
    let last_bytes: &[u8] = &slice[sample_ending_boundary..];

    write!(f, "{{BytesLike(len: {len}. First {SAMPLE_ENDS_COUNT} bytes: {first_bytes:?}. Last {SAMPLE_ENDS_COUNT} bytes: {last_bytes:?})}}")
}

pub fn map_debug_fmt<T, K, V>(map: &T, f: &mut std::fmt::Formatter) -> std::fmt::Result
where
    T: ?Sized,
    for<'a> &'a T: IntoIterator<Item = (&'a K, &'a V)>,
    K: std::fmt::Display,
    V: std::fmt::Debug + AsRef<[u8]>,
{
    write!(f, "{{MapLike: ")?;

    // Use a peekable iterator to handle the trailing comma correctly.
    let mut iter = map.into_iter().peekable();

    while let Some((k, v)) = iter.next() {
        write!(f, "(K: {k}, V: ")?;
        // `v` is a `&V`. Because `V: AsRef<[u8]>`, `&V` also implements `AsRef<[u8]>`.
        // So we can pass `v` directly to our helper.
        bytes_debug_fmt(v, f)?;
        write!(f, ")")?;

        // Only write a comma if this is not the last item.
        if iter.peek().is_some() {
            write!(f, ", ")?;
        }
    }

    write!(f, "}}")
}

/// Thread-safe wrapper for UUID that can be atomically updated
#[derive(Debug)]
pub struct AtomicUuid {
    high: AtomicU64,
    low: AtomicU64,
}

impl AtomicUuid {
    pub fn new(uuid: Uuid) -> Self {
        let bytes = uuid.as_bytes();
        let high = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let low = u64::from_be_bytes(bytes[8..16].try_into().unwrap());

        Self {
            high: AtomicU64::new(high),
            low: AtomicU64::new(low),
        }
    }

    pub fn load(&self, ordering: Ordering) -> Uuid {
        let high = self.high.load(ordering);
        let low = self.low.load(ordering);

        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&high.to_be_bytes());
        bytes[8..16].copy_from_slice(&low.to_be_bytes());

        Uuid::from_bytes(bytes)
    }

    pub fn store(&self, uuid: Uuid, ordering: Ordering) {
        let bytes = uuid.as_bytes();
        let high = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let low = u64::from_be_bytes(bytes[8..16].try_into().unwrap());

        self.high.store(high, ordering);
        self.low.store(low, ordering);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct ConnectSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct ConnectFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

/// Returned when a Connect request is made for a session that is already active.
/// This allows the frontend to gracefully handle the case where the user is already
/// connected (e.g., from another tab or auto-reconnect) without treating it as an error.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct SessionAlreadyActive {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub username: String,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct RegisterSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct RegisterFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct ServiceConnectionAccepted {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct MessageSendSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct MessageSendFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct MessageNotification {
    // The DECRYPTED body of a peer-to-peer message.
    //
    // This was the only `Vec<u8>` in this file with no debug formatter, and
    // `kernel/ext.rs` logs every response with `{:?}`. At `RUST_LOG=debug` --
    // which is the first thing an operator raises when diagnosing delivery --
    // the full plaintext of every message the agent handled went to the log,
    // and from there to whatever collects it and to whatever gets pasted into
    // an issue. The agent exists to keep this material off disk and off the
    // wire; putting it in a log line one level away defeats that.
    //
    // `bytes_debug_fmt` prints the length and the first and last five bytes,
    // which is what every other byte field in this file already does.
    #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
    #[debug(with = plaintext_debug_fmt)]
    pub message: Vec<u8>,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub request_id: Option<Uuid>,
}

/// A media session is live and frames may now be sent.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct MediaSessionOpened {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    /// True when frames travel over UDP. False means the peer connected without
    /// UdpMode Enabled and there is no datagram path — the caller should tell
    /// the user the call cannot start rather than silently sending nothing.
    pub unreliable: bool,
    /// Largest frame the transport will accept, so the encoder can be capped to
    /// something that will actually fit rather than failing per frame.
    pub max_frame_bytes: u32,
    pub request_id: Option<Uuid>,
}

/// A media session could not be opened. Carries why, because "call failed" with
/// no reason is the least actionable thing a user can be told.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct MediaSessionFailed {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct MediaSessionClosed {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub request_id: Option<Uuid>,
}

/// One decoded-ready media frame, reassembled and released in order.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct MediaFrameNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub track: u8,
    pub kind: u8,
    pub sequence: u32,
    pub timestamp: u32,
    pub flags: u8,
    #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
    #[debug(with = bytes_debug_fmt)]
    pub payload: Vec<u8>,
    /// Always None: frames are unsolicited, and like MessageNotification they are
    /// routed to the right tab by `cid`, never by the request that started the
    /// call. The field exists because every response variant carries one.
    pub request_id: Option<Uuid>,
}

/// Frames were lost and the buffer has moved past them.
///
/// Reported rather than hidden: a video decoder that has missed frames produces
/// garbage until the next keyframe, so the receiver needs this to know it should
/// ask for one instead of rendering corruption.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct MediaGapNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub track: u8,
    pub missing_from: u32,
    pub missing_to: u32,
    /// Always None; routed by `cid`, as above.
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct DisconnectNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct DisconnectFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct DeregisterSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct DeregisterFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct SendFileRequestSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct SendFileRequestFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct DownloadFileSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct DownloadFileFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct DeleteVirtualFileSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct DeleteVirtualFileFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PickFileSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    /// The full path to the selected file
    #[cfg_attr(feature = "typescript", ts(type = "string"))]
    pub file_path: PathBuf,
    /// The file name (without path)
    pub file_name: String,
    /// The file size in bytes
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub file_size: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PickFileFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

/// Source for file transfer operations.
/// Allows either a direct file path, a reference to a previously picked file,
/// or inline byte contents (for browser-selected files).
///
/// NOTE: the `Debug` below is `custom_debug::Debug` (imported at the top of
/// this module, shadowing `std`'s), written out explicitly here so it's
/// obvious the `#[debug(with = ...)]` field attribute on `ByteContents.data`
/// is supported — the std derive would not accept it.
#[derive(Serialize, Deserialize, custom_debug::Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub enum FileSource {
    /// Direct file path (for native apps, CLI, or after PickFile)
    Path(#[cfg_attr(feature = "typescript", ts(type = "string"))] PathBuf),
    /// Reference to a PickFile result stored in the internal service.
    /// The pick_file_request_id is the request_id from the PickFile response.
    PickFileRef { pick_file_request_id: Uuid },
    /// Inline byte contents from browser File objects.
    ///
    /// The internal service writes these to a temp file before sending.
    ///
    /// ## Operational cost (WebSocket / JSON path)
    /// Over the WebSocket transport, payloads are encoded as JSON
    /// (`serde_json::to_string`), which encodes `Vec<u8>` as a JSON array
    /// of decimal integers. Each byte typically expands to 2-4 bytes of
    /// JSON text plus separators, and the browser must construct an
    /// equivalent JS `number[]` array in memory. Plan on roughly 3-4x
    /// the raw byte length for transient memory on both sides during a
    /// single request, in addition to the materialised `Vec<u8>` itself.
    ///
    /// The TCP transport uses `bincode2` (binary framing via
    /// `SerializingCodec`) and does not incur this expansion - a `Vec<u8>`
    /// is encoded as length-prefix plus raw bytes (~1:1).
    ///
    /// ## Size cap
    /// The handler caps `data.len()` at 16 MiB. This is the practical
    /// ceiling for the WebSocket/JSON path, where the ~3-4x expansion
    /// approaches the WS frame limit. TCP can in principle accept
    /// larger payloads (the bincode2-framed `LengthDelimitedCodec` cap
    /// is 64 MiB), but the cap is applied uniformly so behaviour does
    /// not depend on which transport happens to be in use. The
    /// browser-side workspace UI applies a much stricter cap (a few MiB)
    /// before invoking this path. Larger uploads should go through the
    /// native `PickFile` flow, which streams from disk and bypasses both
    /// the memory blow-up and the JSON-encoding cost.
    ByteContents {
        file_name: String,
        /// Raw payload bytes of the file.
        // Rust-only (NOT exported by ts-rs — plain `//` comments are
        // skipped by ts-rs but the triple-slash `///` block above is
        // emitted into the generated FileSource.ts as JSDoc): the
        // `bytes_debug_fmt` formatter on the `#[debug]` attribute
        // prevents the full payload from landing in `{:?}` log
        // output — without it a single Debug render of
        // `FileSource::ByteContents` could dump hundreds of MiB into
        // the log. See `bytes_debug_fmt` earlier in this file.
        #[debug(with = bytes_debug_fmt)]
        #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
        data: Vec<u8>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerConnectSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerConnectFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerConnectAcceptSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    /// Which answer was delivered: `true` accepted the connection, `false`
    /// refused it.
    ///
    /// Without this the name is the whole message, and it says "success" for
    /// both — accurate about delivery, silent about the outcome. A receiver
    /// could not tell "they accepted" from "your refusal was sent".
    ///
    /// `PeerRegisterRespond` had exactly that shape and it was a live defect:
    /// declining a registration ran the frontend's acceptance path, marked the
    /// declined peer registered, and had auto-connect open a connection to the
    /// person just refused.
    pub accept: bool,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerConnectAcceptFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerDisconnectSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerDisconnectFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerConnectNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "SessionSecuritySettings"))]
    pub session_security_settings: SessionSecuritySettings,
    #[cfg_attr(feature = "typescript", ts(type = "UdpMode"))]
    pub udp_mode: UdpMode,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerRegisterNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub peer_username: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerRegisterSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub peer_username: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerRegisterFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupChannelCreateSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupChannelCreateFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupBroadcastHandleFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupCreateSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupCreateFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupLeaveSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupLeaveFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupEndSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupEndFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupEndNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub success: bool,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupLeaveNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub success: bool,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupMessageNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    // Length only, for the same reason as MessageNotification above: this is a
    // decrypted body, and a group one reaches more people than a direct one.
    //
    // It carried `bytes_debug_fmt`, which samples the first and last five bytes.
    // That is the right trade for a key or a chunk and the wrong one for a
    // message: five bytes of a chat line is its opening word.
    #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
    #[debug(with = plaintext_debug_fmt)]
    pub message: Vec<u8>,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupMessageSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupMessageResponse {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub success: bool,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupMessageFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupInviteNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupInviteSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupInviteFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupRespondRequestSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupRespondRequestFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupMembershipResponse {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub success: bool,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupRequestJoinPendingNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub result: Result<(), String>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupDisconnectNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupKickSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupKickFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupListGroupsSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey[] | null"))]
    pub group_list: Option<Vec<MessageGroupKey>>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupListGroupsFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupListGroupsResponse {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey[] | null"))]
    pub group_list: Option<Vec<MessageGroupKey>>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupJoinRequestNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupRequestJoinAcceptResponse {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupRequestJoinDeclineResponse {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupRequestJoinSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupRequestJoinFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GroupMemberStateChangeNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
    pub group_key: MessageGroupKey,
    #[cfg_attr(feature = "typescript", ts(type = "MemberState"))]
    pub state: MemberState,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBGetKVSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub key: String,
    #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
    #[debug(with = bytes_debug_fmt)]
    pub value: Vec<u8>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBGetKVFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBSetKVSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub key: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBSetKVFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBDeleteKVSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub key: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBDeleteKVFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBGetAllKVSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    #[cfg_attr(feature = "typescript", ts(type = "Record<string, number[]>"))]
    #[debug(with = map_debug_fmt)]
    pub map: HashMap<String, Vec<u8>>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBGetAllKVFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBClearAllKVSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerInformation {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub online_status: bool,
    pub name: Option<String>,
    pub username: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct ListAllPeersResponse {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "Record<string, PeerInformation>"))]
    pub peer_information: HashMap<u64, PeerInformation>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct ListAllPeersFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct ListRegisteredPeersFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct ListRegisteredPeersResponse {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "Record<string, PeerInformation>"))]
    pub peers: HashMap<u64, PeerInformation>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct LocalDBClearAllKVFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    pub message: String,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct GetSessionsResponse {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub sessions: Vec<SessionInformation>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct FileTransferRequestNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "VirtualObjectMetadata"))]
    pub metadata: VirtualObjectMetadata,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct FileTransferStatusNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "ObjectId"))]
    pub object_id: ObjectId,
    pub success: bool,
    pub response: bool,
    pub message: Option<String>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct FileTransferTickNotification {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
    pub peer_cid: Option<u64>,
    #[cfg_attr(feature = "typescript", ts(type = "ObjectTransferStatus"))]
    pub status: ObjectTransferStatus,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug, Clone, IsError, IsNotification, RequestId, Cid)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub enum InternalServiceResponse {
    ConnectSuccess(ConnectSuccess),
    ConnectFailure(ConnectFailure),
    SessionAlreadyActive(SessionAlreadyActive),
    RegisterSuccess(RegisterSuccess),
    RegisterFailure(RegisterFailure),
    ServiceConnectionAccepted(ServiceConnectionAccepted),
    MessageSendSuccess(MessageSendSuccess),
    MessageSendFailure(MessageSendFailure),
    MessageNotification(MessageNotification),
    MediaSessionOpened(MediaSessionOpened),
    MediaSessionFailed(MediaSessionFailed),
    MediaSessionClosed(MediaSessionClosed),
    MediaFrameNotification(MediaFrameNotification),
    MediaGapNotification(MediaGapNotification),
    DisconnectNotification(DisconnectNotification),
    DisconnectFailure(DisconnectFailure),
    DeregisterSuccess(DeregisterSuccess),
    DeregisterFailure(DeregisterFailure),
    SendFileRequestSuccess(SendFileRequestSuccess),
    SendFileRequestFailure(SendFileRequestFailure),
    FileTransferRequestNotification(FileTransferRequestNotification),
    FileTransferStatusNotification(FileTransferStatusNotification),
    FileTransferTickNotification(FileTransferTickNotification),
    DownloadFileSuccess(DownloadFileSuccess),
    DownloadFileFailure(DownloadFileFailure),
    DeleteVirtualFileSuccess(DeleteVirtualFileSuccess),
    DeleteVirtualFileFailure(DeleteVirtualFileFailure),
    PickFileSuccess(PickFileSuccess),
    PickFileFailure(PickFileFailure),
    PeerConnectSuccess(PeerConnectSuccess),
    PeerConnectFailure(PeerConnectFailure),
    PeerConnectAcceptSuccess(PeerConnectAcceptSuccess),
    PeerConnectAcceptFailure(PeerConnectAcceptFailure),
    PeerConnectNotification(PeerConnectNotification),
    PeerRegisterNotification(PeerRegisterNotification),
    PeerDisconnectSuccess(PeerDisconnectSuccess),
    PeerDisconnectFailure(PeerDisconnectFailure),
    PeerRegisterSuccess(PeerRegisterSuccess),
    PeerRegisterFailure(PeerRegisterFailure),
    GroupChannelCreateSuccess(GroupChannelCreateSuccess),
    GroupChannelCreateFailure(GroupChannelCreateFailure),
    GroupBroadcastHandleFailure(GroupBroadcastHandleFailure),
    GroupCreateSuccess(GroupCreateSuccess),
    GroupCreateFailure(GroupCreateFailure),
    GroupLeaveSuccess(GroupLeaveSuccess),
    GroupLeaveFailure(GroupLeaveFailure),
    GroupLeaveNotification(GroupLeaveNotification),
    GroupEndSuccess(GroupEndSuccess),
    GroupEndFailure(GroupEndFailure),
    GroupEndNotification(GroupEndNotification),
    GroupMessageNotification(GroupMessageNotification),
    GroupMessageResponse(GroupMessageResponse),
    GroupMessageSuccess(GroupMessageSuccess),
    GroupMessageFailure(GroupMessageFailure),
    GroupInviteNotification(GroupInviteNotification),
    GroupInviteSuccess(GroupInviteSuccess),
    GroupInviteFailure(GroupInviteFailure),
    GroupRespondRequestSuccess(GroupRespondRequestSuccess),
    GroupRespondRequestFailure(GroupRespondRequestFailure),
    GroupMembershipResponse(GroupMembershipResponse),
    GroupRequestJoinPendingNotification(GroupRequestJoinPendingNotification),
    GroupDisconnectNotification(GroupDisconnectNotification),
    GroupKickSuccess(GroupKickSuccess),
    GroupKickFailure(GroupKickFailure),
    GroupListGroupsSuccess(GroupListGroupsSuccess),
    GroupListGroupsFailure(GroupListGroupsFailure),
    GroupListGroupsResponse(GroupListGroupsResponse),
    GroupJoinRequestNotification(GroupJoinRequestNotification),
    GroupRequestJoinAcceptResponse(GroupRequestJoinAcceptResponse),
    GroupRequestJoinDeclineResponse(GroupRequestJoinDeclineResponse),
    GroupRequestJoinSuccess(GroupRequestJoinSuccess),
    GroupRequestJoinFailure(GroupRequestJoinFailure),
    GroupMemberStateChangeNotification(GroupMemberStateChangeNotification),
    LocalDBGetKVSuccess(LocalDBGetKVSuccess),
    LocalDBGetKVFailure(LocalDBGetKVFailure),
    LocalDBSetKVSuccess(LocalDBSetKVSuccess),
    LocalDBSetKVFailure(LocalDBSetKVFailure),
    LocalDBDeleteKVSuccess(LocalDBDeleteKVSuccess),
    LocalDBDeleteKVFailure(LocalDBDeleteKVFailure),
    LocalDBGetAllKVSuccess(LocalDBGetAllKVSuccess),
    LocalDBGetAllKVFailure(LocalDBGetAllKVFailure),
    LocalDBClearAllKVSuccess(LocalDBClearAllKVSuccess),
    LocalDBClearAllKVFailure(LocalDBClearAllKVFailure),
    GetSessionsResponse(GetSessionsResponse),
    GetAccountInformationResponse(Accounts),
    ListAllPeersResponse(ListAllPeersResponse),
    ListAllPeersFailure(ListAllPeersFailure),
    ListRegisteredPeersResponse(ListRegisteredPeersResponse),
    ListRegisteredPeersFailure(ListRegisteredPeersFailure),
    ConnectionManagementSuccess(ConnectionManagementSuccess),
    ConnectionManagementFailure(ConnectionManagementFailure),
    /// Results from a batched request, in the same order as input commands
    BatchedResponse(BatchedResponseData),
}

#[derive(Serialize, Deserialize, Debug, Clone, RequestId)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub enum InternalServiceRequest {
    Connect {
        request_id: Uuid,
        username: String,
        #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
        password: SecBuffer,
        #[cfg_attr(feature = "typescript", ts(type = "ConnectMode"))]
        connect_mode: ConnectMode,
        #[cfg_attr(feature = "typescript", ts(type = "UdpMode"))]
        udp_mode: UdpMode,
        #[cfg_attr(
            feature = "typescript",
            ts(type = "{ secs: number; nanos: number } | null")
        )]
        keep_alive_timeout: Option<Duration>,
        #[cfg_attr(feature = "typescript", ts(type = "SessionSecuritySettings"))]
        session_security_settings: SessionSecuritySettings,
        #[cfg_attr(feature = "typescript", ts(type = "PreSharedKey | null"))]
        server_password: Option<PreSharedKey>,
    },
    Register {
        request_id: Uuid,
        /// `host:port`, resolved by the AGENT rather than by the browser.
        ///
        /// This was a `SocketAddr`, so the page had to resolve a hostname
        /// before it could register -- and it did so with a DNS-over-HTTPS
        /// fetch to `https://dns.google/resolve`. A hosted UI's own
        /// Content-Security-Policy refuses that connection, so every hostname
        /// address failed with a 30-second timeout while a raw IP worked; and
        /// where it did not fail it told Google which server each user was
        /// joining.
        ///
        /// The agent has a resolver and no CSP.
        ///
        /// Compatibility is NOT symmetric, and the first version of this
        /// comment claimed it was. Serde renders a `SocketAddr` as exactly this
        /// string, so an IP:port sent by either side parses on either side --
        /// but a client still typed `SocketAddr` cannot DESERIALIZE a hostname,
        /// which is the only case this change exists for. A browser holding a
        /// WASM build from before it refuses `citadel.avarok.net:12400` with
        /// `Deserialization error: invalid socket address syntax`, in the
        /// browser, before the request is ever sent -- so the agent logs
        /// nothing and the user waits out the 30s registration timeout.
        ///
        /// The UI bundle and its WASM client therefore ship together. That is
        /// already true of every build the pipeline produces; it is written
        /// down because the failure names neither the cause nor the component.
        #[cfg_attr(feature = "typescript", ts(type = "string"))]
        server_addr: String,
        full_name: String,
        username: String,
        #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
        proposed_password: SecBuffer,
        connect_after_register: bool,
        #[cfg_attr(feature = "typescript", ts(type = "SessionSecuritySettings"))]
        session_security_settings: SessionSecuritySettings,
        #[cfg_attr(feature = "typescript", ts(type = "PreSharedKey | null"))]
        server_password: Option<PreSharedKey>,
    },
    Message {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
        // Length only, like the inbound counterpart. This is the SAME material
        // -- the user's decrypted message body -- and it was printing its first
        // and last five bytes, which for a chat line is its opening word. The
        // response side already learned this; the request side had not, which
        // is how the original leak happened.
        #[debug(with = plaintext_debug_fmt)]
        message: Vec<u8>,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
        #[cfg_attr(feature = "typescript", ts(type = "SecurityLevel"))]
        security_level: SecurityLevel,
    },
    Disconnect {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
    },

    /// Open a media (audio/video) session with an already-connected peer.
    ///
    /// Media rides the session's UDP channel, NOT the reliable peer channel the
    /// messages above use. Two reasons: a call must not queue behind chat, and
    /// on a reliable ordered channel congestion turns into unbounded latency
    /// rather than loss, which is far worse for a call than a dropped frame.
    ///
    /// Requires the peer connection to have been established with UdpMode
    /// Enabled; the response reports failure if no UDP channel arrives.
    MediaOpen {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
    },

    /// Send one encoded media frame to a peer.
    ///
    /// The payload is opaque: encoding and decoding happen in the browser via
    /// WebCodecs, because that is the only path to hardware acceleration and
    /// this crate's WASM build has neither threads nor SIMD. The service only
    /// fragments, orders and transports.
    MediaSend {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
        /// Which stream within the call: audio, main video, or thumbnail video.
        track: u8,
        /// 0 = audio, 1 = video. Mirrors citadel_media's TrackKind.
        kind: u8,
        /// Capture time in the track's clock rate, used for A/V sync.
        timestamp: u32,
        /// Bit 0 = keyframe, bit 1 = discardable under congestion.
        flags: u8,
        #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
        #[debug(with = bytes_debug_fmt)]
        payload: Vec<u8>,
    },

    /// Tear down a media session, releasing the UDP channel and its pump task.
    MediaClose {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
    },
    /// Deregister from the server - permanently removes the account
    Deregister {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
    },
    SendFile {
        request_id: Uuid,
        /// File source - either a direct path or a reference to a PickFile result.
        /// Use FileSource::Path for direct file paths, or FileSource::PickFileRef
        /// to reference a previously picked file via its request_id.
        source: FileSource,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
        chunk_size: Option<usize>,
        #[cfg_attr(feature = "typescript", ts(type = "TransferType"))]
        transfer_type: TransferType,
    },
    RespondFileTransfer {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "ObjectId"))]
        object_id: ObjectId,
        accept: bool,
        #[cfg_attr(feature = "typescript", ts(type = "string | null"))]
        download_location: Option<PathBuf>,
        request_id: Uuid,
    },
    DownloadFile {
        #[cfg_attr(feature = "typescript", ts(type = "string"))]
        virtual_directory: PathBuf,
        #[cfg_attr(feature = "typescript", ts(type = "SecurityLevel | null"))]
        security_level: Option<SecurityLevel>,
        delete_on_pull: bool,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
        request_id: Uuid,
    },
    DeleteVirtualFile {
        #[cfg_attr(feature = "typescript", ts(type = "string"))]
        virtual_directory: PathBuf,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
        request_id: Uuid,
    },
    /// Opens a native file picker dialog to select a file.
    /// Returns the full file path, name, and size.
    /// This runs on the native internal-service (not WASM) so it has full filesystem access.
    PickFile {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        /// Optional title for the file picker dialog
        title: Option<String>,
        /// Optional list of allowed file extensions (e.g., ["pdf", "txt"])
        #[cfg_attr(feature = "typescript", ts(type = "string[] | null"))]
        allowed_extensions: Option<Vec<String>>,
    },
    ListAllPeers {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
    },
    ListRegisteredPeers {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
    },
    PeerConnect {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "UdpMode"))]
        udp_mode: UdpMode,
        #[cfg_attr(feature = "typescript", ts(type = "SessionSecuritySettings"))]
        session_security_settings: SessionSecuritySettings,
        #[cfg_attr(feature = "typescript", ts(type = "PreSharedKey | null"))]
        peer_session_password: Option<PreSharedKey>,
    },
    PeerDisconnect {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
    },
    /// Accept an incoming P2P connection request from a peer.
    /// This is sent in response to PeerConnectNotification to complete the handshake.
    PeerConnectAccept {
        request_id: Uuid,
        /// CID of the local session accepting the connection
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        /// CID of the peer who initiated the connection
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
        /// Accept (true) or decline (false) the connection
        accept: bool,
        #[cfg_attr(feature = "typescript", ts(type = "UdpMode"))]
        udp_mode: UdpMode,
        #[cfg_attr(feature = "typescript", ts(type = "SessionSecuritySettings"))]
        session_security_settings: SessionSecuritySettings,
        #[cfg_attr(feature = "typescript", ts(type = "PreSharedKey | null"))]
        peer_session_password: Option<PreSharedKey>,
    },
    PeerRegister {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "SessionSecuritySettings"))]
        session_security_settings: SessionSecuritySettings,
        connect_after_register: bool,
        #[cfg_attr(feature = "typescript", ts(type = "PreSharedKey | null"))]
        peer_session_password: Option<PreSharedKey>,
    },
    /// Respond to an incoming peer registration request (accept/decline).
    /// Used when a peer sends a PeerRegister and we receive PeerRegisterNotification.
    PeerRegisterRespond {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
        accept: bool,
    },
    LocalDBGetKV {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
        key: String,
    },
    LocalDBSetKV {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
        key: String,
        #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
        #[debug(with = bytes_debug_fmt)]
        value: Vec<u8>,
    },
    LocalDBDeleteKV {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
        key: String,
    },
    LocalDBGetAllKV {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
    },
    LocalDBClearAllKV {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
    },
    GetSessions {
        request_id: Uuid,
    },
    GetAccountInformation {
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        cid: Option<u64>,
    },
    GroupCreate {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        request_id: Uuid,
        #[cfg_attr(feature = "typescript", ts(type = "UserIdentifier[] | null"))]
        initial_users_to_invite: Option<Vec<UserIdentifier>>,
    },
    GroupLeave {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupEnd {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupMessage {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "number[]"))]
        // Length only, like the inbound counterpart. This is the SAME material
        // -- the user's decrypted message body -- and it was printing its first
        // and last five bytes, which for a chat line is its opening word. The
        // response side already learned this; the request side had not, which
        // is how the original leak happened.
        #[debug(with = plaintext_debug_fmt)]
        message: Vec<u8>,
        #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupInvite {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupRespondRequest {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
        group_key: MessageGroupKey,
        response: bool,
        request_id: Uuid,
        invitation: bool,
    },
    GroupKick {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        peer_cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    GroupListGroupsFor {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        peer_cid: Option<u64>,
        request_id: Uuid,
    },
    GroupRequestJoin {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        cid: u64,
        #[cfg_attr(feature = "typescript", ts(type = "MessageGroupKey"))]
        group_key: MessageGroupKey,
        request_id: Uuid,
    },
    ConnectionManagement {
        request_id: Uuid,
        management_command: ConfigCommand,
    },
    /// Execute multiple requests in parallel, returning results in the same order as input.
    /// This enables single-roundtrip batch operations for efficiency.
    Batched {
        request_id: Uuid,
        /// The list of commands to execute in parallel
        commands: Vec<InternalServiceRequest>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct ConnectionManagementSuccess {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
    pub message: String,
}

/// Response from a batched request containing results in the same order as input commands
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct BatchedResponseData {
    /// CID is 0 for batched responses (batch is not tied to a single session)
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
    pub results: Vec<InternalServiceResponse>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct ConnectionManagementFailure {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub request_id: Option<Uuid>,
    pub error: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub enum ConfigCommand {
    SetConnectionOrphan {
        allow_orphan_sessions: bool,
    },
    ClaimSession {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        session_cid: u64,
        only_if_orphaned: bool,
    },
    DisconnectOrphan {
        #[cfg_attr(feature = "typescript", ts(type = "bigint | null"))]
        session_cid: Option<u64>,
    },
    /// Release a session, marking it as orphaned without disconnecting.
    /// Called when the last browser tab with this CID closes.
    /// The session stays in server_connection_map but becomes immediately claimable.
    ReleaseSession {
        #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
        session_cid: u64,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct SessionInformation {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    pub username: String,
    pub server_address: String,
    #[cfg_attr(
        feature = "typescript",
        ts(type = "Record<string, PeerSessionInformation>")
    )]
    pub peer_connections: HashMap<u64, PeerSessionInformation>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct Accounts {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(
        feature = "typescript",
        ts(type = "Record<string, AccountInformation>")
    )]
    pub accounts: HashMap<u64, AccountInformation>,
    pub request_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct AccountInformation {
    pub username: String,
    pub full_name: String,
    #[cfg_attr(
        feature = "typescript",
        ts(type = "Record<string, PeerSessionInformation>")
    )]
    pub peers: HashMap<u64, PeerSessionInformation>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub struct PeerSessionInformation {
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub cid: u64,
    #[cfg_attr(feature = "typescript", ts(type = "bigint"))]
    pub peer_cid: u64,
    pub peer_username: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "typescript", derive(TS))]
#[cfg_attr(feature = "typescript", ts(export))]
pub enum InternalServicePayload {
    Request(InternalServiceRequest),
    Response(InternalServiceResponse),
}

impl From<InternalServiceResponse> for InternalServicePayload {
    fn from(response: InternalServiceResponse) -> Self {
        InternalServicePayload::Response(response)
    }
}

impl From<InternalServiceRequest> for InternalServicePayload {
    fn from(request: InternalServiceRequest) -> Self {
        InternalServicePayload::Request(request)
    }
}

impl InternalServiceRequest {
    /// The session this request acts on, when it names one.
    ///
    /// Used to check that the caller owns the session before the request is
    /// dispatched. Every handler previously took `cid` straight off the wire and
    /// acted on it, while the connection's own identity sat unused in scope — so
    /// a request could name any session it liked. WebSocket is exempt from CORS,
    /// which made that reachable from any page a user happened to visit.
    ///
    /// `None` for the six variants that legitimately precede or span a session:
    /// Connect, Register, GetSessions, GetAccountInformation,
    /// ConnectionManagement (which is how a session is claimed in the first
    /// place) and Batched (whose inner commands are each checked on dispatch).
    pub fn session_cid(&self) -> Option<u64> {
        match self {
            Self::Message { cid, .. } => Some(*cid),
            Self::Disconnect { cid, .. } => Some(*cid),
            Self::MediaOpen { cid, .. } => Some(*cid),
            Self::MediaSend { cid, .. } => Some(*cid),
            Self::MediaClose { cid, .. } => Some(*cid),
            Self::Deregister { cid, .. } => Some(*cid),
            Self::SendFile { cid, .. } => Some(*cid),
            Self::RespondFileTransfer { cid, .. } => Some(*cid),
            Self::DownloadFile { cid, .. } => Some(*cid),
            Self::DeleteVirtualFile { cid, .. } => Some(*cid),
            Self::PickFile { cid, .. } => Some(*cid),
            Self::ListAllPeers { cid, .. } => Some(*cid),
            Self::ListRegisteredPeers { cid, .. } => Some(*cid),
            Self::PeerConnect { cid, .. } => Some(*cid),
            Self::PeerDisconnect { cid, .. } => Some(*cid),
            Self::PeerConnectAccept { cid, .. } => Some(*cid),
            Self::PeerRegister { cid, .. } => Some(*cid),
            Self::PeerRegisterRespond { cid, .. } => Some(*cid),
            Self::LocalDBGetKV { cid, .. } => Some(*cid),
            Self::LocalDBSetKV { cid, .. } => Some(*cid),
            Self::LocalDBDeleteKV { cid, .. } => Some(*cid),
            Self::LocalDBGetAllKV { cid, .. } => Some(*cid),
            Self::LocalDBClearAllKV { cid, .. } => Some(*cid),
            Self::GroupCreate { cid, .. } => Some(*cid),
            Self::GroupLeave { cid, .. } => Some(*cid),
            Self::GroupEnd { cid, .. } => Some(*cid),
            Self::GroupMessage { cid, .. } => Some(*cid),
            Self::GroupInvite { cid, .. } => Some(*cid),
            Self::GroupRespondRequest { cid, .. } => Some(*cid),
            Self::GroupKick { cid, .. } => Some(*cid),
            Self::GroupListGroupsFor { cid, .. } => Some(*cid),
            Self::GroupRequestJoin { cid, .. } => Some(*cid),
            // Exhaustive on purpose: no `_` arm.
            //
            // The catch-all made this gate fail OPEN by omission — a variant
            // added later would silently be exempt from the ownership check in
            // requests/mod.rs, with nothing to notice. Naming the six turns
            // that into a compile error for the next variant, which is the only
            // reliable place to catch it.
            //
            // These six legitimately precede or span a session:
            //   Connect / Register     — there is no session yet.
            //   Batched                — the inner requests are gated one by one.
            //   ConnectionManagement   — carries its target inside
            //                            `management_command`; gated in
            //                            requests/connection_management_auth.rs.
            //   GetSessions /
            //   GetAccountInformation  — enumerate what this agent holds.
            Self::Connect { .. }
            | Self::Register { .. }
            | Self::Batched { .. }
            | Self::ConnectionManagement { .. }
            | Self::GetSessions { .. }
            | Self::GetAccountInformation { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_error_derive() {
        let success_response = InternalServiceResponse::ConnectSuccess(ConnectSuccess {
            cid: 0,
            request_id: None,
        });
        let error_response = InternalServiceResponse::ConnectFailure(ConnectFailure {
            cid: 0,
            message: "test".to_string(),
            request_id: None,
        });
        assert!(!success_response.is_error());
        assert!(error_response.is_error());
    }

    #[test]
    fn test_is_notification_derive() {
        let success_response = InternalServiceResponse::ConnectSuccess(ConnectSuccess {
            cid: 0,
            request_id: None,
        });
        let notification_response =
            InternalServiceResponse::PeerRegisterNotification(PeerRegisterNotification {
                cid: 0,
                peer_cid: 0,
                peer_username: "".to_string(),
                request_id: None,
            });
        assert!(!success_response.is_notification());
        assert!(notification_response.is_notification());
    }

    #[test]
    fn test_request_id_derive() {
        let request_id = Uuid::new_v4();
        let request = InternalServiceRequest::Connect {
            request_id,
            username: "test".to_string(),
            password: SecBuffer::from(vec![]),
            connect_mode: ConnectMode::default(),
            udp_mode: UdpMode::Enabled,
            keep_alive_timeout: None,
            session_security_settings: SessionSecuritySettings::default(),
            server_password: None,
        };
        assert_eq!(request.request_id(), Some(&request_id));
    }

    #[test]
    fn test_cid_derive() {
        let cid = 1234;
        let request = InternalServiceResponse::ConnectSuccess(ConnectSuccess {
            cid,
            request_id: None,
        });

        assert_eq!(request.cid(), cid);
    }

    // Test that triggers TypeScript export when running tests with typescript feature
    #[cfg(feature = "typescript")]
    #[test]
    fn trigger_typescript_export() {
        use ts_rs::TS;

        // Access type information to trigger export
        let _ = InternalServiceRequest::name();
        let _ = InternalServiceResponse::name();
        let _ = InternalServicePayload::name();
    }
}

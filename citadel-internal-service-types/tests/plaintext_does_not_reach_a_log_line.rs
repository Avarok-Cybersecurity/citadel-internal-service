//! Nothing that carries secret bytes may print them under `{:?}`.
//!
//! `kernel/ext.rs` logs every response the agent sends with
//! `debug!("Sending kernel response to client: {:?}", ...)`. That is a reasonable
//! thing for it to do — provided the response types redact what they carry.
//!
//! `MessageNotification.message` did not. It is the DECRYPTED body of a
//! peer-to-peer message, it was the only `Vec<u8>` in the crate with no debug
//! formatter, and at `RUST_LOG=debug` — the first thing an operator raises when
//! diagnosing delivery — the full plaintext of every message the agent handled
//! went to the log, and from there to whatever collects it and to whatever gets
//! pasted into an issue.
//!
//! These assert the property directly: format the value, and look for the bytes.
//! A test that asserted the ATTRIBUTE was present would pass if the attribute were
//! spelled correctly and did nothing.

use citadel_internal_service_types::{
    GroupMessageNotification, InternalServiceResponse, MessageGroupKey, MessageNotification,
};
use uuid::Uuid;

/// Long enough that `bytes_debug_fmt` elides the middle rather than printing all
/// of it — short values are shown whole by design, so a short body would let this
/// pass for the wrong reason.
fn secret_body() -> Vec<u8> {
    b"MEET ME AT THE PIER AT MIDNIGHT AND BRING THE LEDGER".to_vec()
}

fn notification() -> MessageNotification {
    MessageNotification {
        message: secret_body(),
        cid: 1,
        peer_cid: 2,
        request_id: Some(Uuid::nil()),
    }
}

#[test]
fn a_decrypted_message_body_is_not_printed_by_debug() {
    let rendered = format!("{:?}", notification());

    let plaintext = String::from_utf8(secret_body()).expect("ascii");
    assert!(
        !rendered.contains(&plaintext),
        "the decrypted body appeared in a Debug rendering, which is what the agent logs:\n{rendered}"
    );

    // And not as a byte array either, which is how a `Vec<u8>` prints by default.
    assert!(
        !rendered.contains("77, 69, 69, 84"),
        "the body appeared as a byte array:\n{rendered}"
    );
}

/// The bytes as `Vec<u8>`'s own Debug renders them: `77, 69, 69, 84, 32`.
///
/// Asserting on the ASCII alone is an assertion that CANNOT FAIL. `format!("{:?}")`
/// of a `Vec<u8>` prints decimal numbers, never the characters, so a completely
/// unredacted field contains no ASCII to find. The first version of the enum test
/// below did exactly that and passed with the formatter deleted — caught by running
/// the control and reading which tests moved, not by reading the test.
fn secret_as_debug_bytes() -> String {
    let bytes = secret_body();
    bytes
        .iter()
        .map(|b| b.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[test]
fn the_same_holds_once_it_is_wrapped_in_the_response_enum() {
    // What `ext.rs` actually formats is the enum, not the struct. A formatter on
    // the field is inherited through the variant, but asserting it here means the
    // test covers the value that is really logged rather than a stand-in.
    let rendered = format!(
        "{:?}",
        InternalServiceResponse::MessageNotification(notification())
    );
    assert!(
        !rendered.contains(&secret_as_debug_bytes()),
        "the decrypted body appeared in the logged response:\n{rendered}"
    );
}

#[test]
fn a_group_message_body_is_redacted_too() {
    // A group body reaches more people than a direct one, and carried the
    // sampling formatter rather than the length-only one.
    let rendered = format!(
        "{:?}",
        InternalServiceResponse::GroupMessageNotification(GroupMessageNotification {
            cid: 1,
            peer_cid: 2,
            message: secret_body(),
            group_key: MessageGroupKey::new(1, 7),
            request_id: Some(Uuid::nil()),
        })
    );
    assert!(
        !rendered.contains(&secret_as_debug_bytes()),
        "the decrypted group body appeared in the logged response:\n{rendered}"
    );
    // Sampling leaks the opening word; assert the first five bytes are gone too.
    let opening = secret_as_debug_bytes()
        .split(", ")
        .take(5)
        .collect::<Vec<_>>()
        .join(", ");
    assert!(
        !rendered.contains(&opening),
        "the opening bytes of the group body survived:\n{rendered}"
    );
}

#[test]
fn the_rendering_still_says_enough_to_diagnose_with() {
    // The control. Redaction that removed everything would pass both tests above
    // and make the log useless, so this pins what a reader still gets: the length,
    // and the routing information that says which session and peer it belonged to.
    let rendered = format!("{:?}", notification());

    assert!(
        rendered.contains(&secret_body().len().to_string()),
        "the length must survive, or the log cannot show a truncated or empty body:\n{rendered}"
    );
    assert!(
        rendered.contains("cid: 1"),
        "the session must still be identifiable:\n{rendered}"
    );
    assert!(
        rendered.contains("peer_cid: 2"),
        "the peer must still be identifiable:\n{rendered}"
    );
}

/// `SecBuffer` redacts itself, and this asserts the SDK still does that.
///
/// The password fields in this crate carry no `#[debug(with = …)]`, and they do
/// not need one: `citadel_types`' `SecBuffer` implements `Debug` as
/// `***SECRET***`. check-byte-fields-do-not-print-themselves.mjs exempts the type
/// for that reason, which makes the exemption only as good as the dependency.
///
/// So the dependency is pinned here rather than trusted: a bump that derived
/// `Debug` instead would fail this, rather than quietly putting account passwords
/// into the same log line the rest of this file is about.
#[test]
fn secbuffer_still_redacts_itself() {
    use citadel_internal_service_types::SecBuffer;

    let secret = "hunter2-and-then-some";
    let rendered = format!("{:?}", SecBuffer::from(secret.as_bytes().to_vec()));

    assert!(
        !rendered.contains(secret),
        "SecBuffer printed its contents; the password fields in this crate rely on it not doing that:\n{rendered}"
    );
    assert!(
        !rendered.contains("104, 117, 110"),
        "SecBuffer printed its bytes:\n{rendered}"
    );
}

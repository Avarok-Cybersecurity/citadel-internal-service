//! Every request the ownership gate can refuse must get a reply.
//!
//! `gate_decision` refuses any request whose `session_cid()` is `Some` and
//! whose session is owned by another connection. That is 32 variants.
//! `refusal_response` answered SIX, and the rest hit `_ => return None`, which
//! sends nothing: the caller waits out its whole budget — thirty seconds for a
//! P2P request — and reports a timeout, which names the wrong cause. The
//! service did not fail to answer; it decided not to act and did not say so.
//!
//! That is reachable in ordinary use. A second browser signs in with the
//! password, `connect.rs` re-points the session to it, and the first browser's
//! next message, file send, call or group action is refused in silence.
//!
//! There WAS a test for this, and it passed the whole time:
//!
//! ```ignore
//! for command in gated_requests(Uuid::new_v4(), 1) {
//!     assert!(requires_owned_session(&command));
//!     assert!(refusal_response(&command, Uuid::new_v4()).is_some());
//! }
//! ```
//!
//! The loop is right. Its input is a hand-written fixture holding the same six
//! variants the function answered, so it asserted a correspondence between two
//! lists that were kept in step with each other and with nothing else.
//!
//! This reads the SOURCE instead, because Rust cannot enumerate enum variants
//! without a derive the types crate does not carry. It is a text check, and it
//! is exhaustive in the way the fixture was not: a variant added to
//! `session_cid`'s `Some` arms fails here until somebody decides what its
//! refusal looks like, or records why it has none.

use std::fs::read_to_string;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is citadel-internal-service/; the types crate is a sibling.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

/// Variant names whose `session_cid()` returns `Some`, read from the types crate.
fn gated_variants() -> Vec<String> {
    let src = read_to_string(repo_root().join("citadel-internal-service-types/src/lib.rs"))
        .expect("types crate source");
    let start = src
        .find("fn session_cid(&self)")
        .expect("session_cid exists");
    let body = &src[start..];
    let end = body.find("\n    }").expect("session_cid ends");
    let body = &body[..end];

    // Line-wise, accumulating the names seen since the last arm ended.
    //
    // The first version split the body on commas, which is wrong for exactly
    // the shape being parsed: `Self::PeerConnect { cid, .. } => Some(*cid),`
    // splits inside the braces, so the fragment carrying `=>` no longer
    // contains `Self::` and every arm was missed. It parsed zero variants — and
    // the floor below is the only reason that was a failure rather than a pass
    // over an empty list.
    let mut gated = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    for line in body.lines() {
        for token in line.split("Self::").skip(1) {
            let name: String = token
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                pending.push(name);
            }
        }
        if let Some((_, right)) = line.split_once("=>") {
            if right.trim_start().starts_with("Some") {
                gated.append(&mut pending);
            }
            pending.clear();
        }
    }
    gated.sort();
    gated.dedup();
    gated
}

#[test]
fn every_gated_request_is_named_in_refusal_response() {
    let gated = gated_variants();
    assert!(
        gated.len() >= 20,
        "parsed only {} gated variants — the parse broke, and an empty list \
         would pass every assertion below",
        gated.len()
    );

    let refusal =
        read_to_string(repo_root().join("citadel-internal-service/src/kernel/requests/mod.rs"))
            .expect("requests/mod.rs");
    let start = refusal
        .find("fn refusal_response(")
        .expect("refusal_response exists");
    let body = &refusal[start..];
    let end = body.find("\n}").expect("refusal_response ends");
    let body = &body[..end];

    let missing: Vec<&String> = gated
        .iter()
        .filter(|name| !body.contains(&format!("InternalServiceRequest::{name} ")))
        .collect();

    assert!(
        missing.is_empty(),
        "these gated request(s) are refused in silence: {missing:?}\n\
         Each one leaves the caller waiting out its full timeout for a decision \
         the service already made. Give it its Failure variant, or add it to the \
         documented silent list with the reason it has no answer to give."
    );
}

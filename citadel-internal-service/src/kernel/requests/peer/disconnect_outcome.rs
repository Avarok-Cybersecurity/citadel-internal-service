//! What to answer once the SDK disconnect has been attempted.
//!
//! Extracted so the decision can be tested without a live protocol stack. The
//! handler around it needs a real `NodeRemote`, an authenticated session and a
//! peer, so the branch that matters was reachable only by running the whole
//! agent -- which is why it went unexamined.
//!
//! The rule: the map entry is removed BEFORE the SDK is asked to disconnect, so
//! reporting success when the SDK refused or timed out leaves the caller wedged.
//! `connect` then finds no entry, calls `remote.connect()`, and the protocol
//! answers `SessionManagerSessionAlreadyExists`; `ClaimSession` and
//! `DisconnectOrphan` both answer "not found" because the entry is gone. No wire
//! command can reach the session that is still there, until the agent restarts.
//!
//! Both failure arms previously logged "Proceeding anyway" and fell through to
//! the success notification. `connection_management.rs` reports its equivalent
//! failure and says in its own comment that this file "has always done this
//! correctly -- so this is that fix, propagated". It was not: this file awaited
//! the result and discarded it. The fix was propagated from a file that never
//! had it, and the claim is why nobody re-read it.

use citadel_internal_service_types::{
    DisconnectNotification, InternalServiceResponse, PeerDisconnectFailure,
};
use std::time::Duration;
use uuid::Uuid;

/// How the SDK's disconnect attempt ended.
#[derive(Debug)]
pub enum SdkDisconnect {
    /// The protocol session is gone.
    Succeeded,
    /// The SDK refused; carries its rendered error.
    Failed(String),
    /// The SDK did not answer within the budget.
    TimedOut,
}

impl SdkDisconnect {
    /// The response to send. Success only when the session actually went away.
    pub fn into_response(
        self,
        cid: u64,
        peer_cid: Option<u64>,
        request_id: Uuid,
        budget: Duration,
    ) -> InternalServiceResponse {
        let message = match self {
            SdkDisconnect::Succeeded => {
                return InternalServiceResponse::DisconnectNotification(DisconnectNotification {
                    cid,
                    peer_cid,
                    request_id: Some(request_id),
                })
            }
            SdkDisconnect::Failed(err) => format!("SDK disconnect failed: {err}"),
            SdkDisconnect::TimedOut => {
                format!("SDK disconnect timed out after {budget:?}")
            }
        };

        InternalServiceResponse::PeerDisconnectFailure(PeerDisconnectFailure {
            cid,
            message,
            request_id: Some(request_id),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUDGET: Duration = Duration::from_secs(10);

    fn response_of(outcome: SdkDisconnect) -> InternalServiceResponse {
        outcome.into_response(7, Some(9), Uuid::nil(), BUDGET)
    }

    #[test]
    fn a_refused_disconnect_is_not_reported_as_a_sign_out() {
        // The defect: this returned DisconnectNotification, so the UI signed the
        // user out while the protocol session survived -- and the map entry was
        // already gone, so nothing could reach it afterwards.
        match response_of(SdkDisconnect::Failed("channel closed".into())) {
            InternalServiceResponse::PeerDisconnectFailure(f) => {
                assert_eq!(f.cid, 7);
                assert!(
                    f.message.contains("channel closed"),
                    "the reason must survive into the message: {}",
                    f.message
                );
            }
            other => panic!("a refused disconnect must not report success: {other:?}"),
        }
    }

    #[test]
    fn a_timed_out_disconnect_is_not_reported_as_a_sign_out() {
        match response_of(SdkDisconnect::TimedOut) {
            InternalServiceResponse::PeerDisconnectFailure(f) => {
                assert!(
                    f.message.contains("timed out"),
                    "the caller should be able to tell a timeout from a refusal: {}",
                    f.message
                );
            }
            other => panic!("a timed-out disconnect must not report success: {other:?}"),
        }
    }

    #[test]
    fn a_real_disconnect_is_still_reported_as_one() {
        // The control. A decision that answered PeerDisconnectFailure for every
        // outcome would satisfy both assertions above and break sign-out
        // entirely -- the UI would never see the notification it waits for.
        match response_of(SdkDisconnect::Succeeded) {
            InternalServiceResponse::DisconnectNotification(n) => {
                assert_eq!(n.cid, 7);
                assert_eq!(n.peer_cid, Some(9));
            }
            other => panic!("a successful disconnect must report success: {other:?}"),
        }
    }

    #[test]
    fn the_request_id_is_carried_on_every_outcome() {
        // Without it the caller cannot correlate the answer with its request,
        // and a failure that nobody can attribute is close to a failure nobody
        // sees.
        for outcome in [
            SdkDisconnect::Succeeded,
            SdkDisconnect::Failed("x".into()),
            SdkDisconnect::TimedOut,
        ] {
            let id = match outcome.into_response(1, None, Uuid::nil(), BUDGET) {
                InternalServiceResponse::DisconnectNotification(n) => n.request_id,
                InternalServiceResponse::PeerDisconnectFailure(f) => f.request_id,
                other => panic!("unexpected variant: {other:?}"),
            };
            assert_eq!(id, Some(Uuid::nil()));
        }
    }
}

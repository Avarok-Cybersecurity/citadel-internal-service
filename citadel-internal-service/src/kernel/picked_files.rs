//! The file-picker results a session may still refer to.
//!
//! `PickFile` opens a native picker and records the chosen path so that a later
//! `SendFile` can name it by request id instead of re-sending the path. The map
//! that held those records was insert-only: no code path anywhere removed an
//! entry. Every file a user ever picked stayed in memory for the life of the
//! session — and sessions here deliberately survive TCP drops, refreshes and
//! closed tabs, so "the life of the session" is unbounded.
//!
//! `PickedFileInfo` already carried `picked_at`, described in its own comment as
//! being "for expiration/cleanup", and nothing read it. The lookup failure in
//! `upload.rs` already told the user "the file picker result may have expired",
//! describing an expiry that did not exist — so the one diagnosis the user was
//! given named a cause that could not occur, and sent them to re-pick a file
//! when the real reason was a different session or a restarted agent.
//!
//! This implements what both of those already claimed. A pick is usable for
//! `PICKED_FILE_TTL`, entries older than that are swept whenever a new one is
//! stored, and the map is capped so a burst of picks cannot grow it without
//! bound even inside one TTL window.
//!
//! Entries are NOT consumed on use: sending one picked file to two peers, or
//! retrying a failed send, are both ordinary and neither should require the
//! user to re-open a picker.

use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

use super::PickedFileInfo;

/// How long a picker result may be referred to by a later `SendFile`.
///
/// Long enough that picking a file and then choosing a recipient, being
/// interrupted, and coming back is normal; short enough that an abandoned pick
/// does not pin a path for a session that lasts days.
pub const PICKED_FILE_TTL: Duration = Duration::from_secs(30 * 60);

/// Most picks one session may hold at once, oldest evicted first.
///
/// `allow(dead_code)` without `native-dialogs`, not a cfg: the picker itself is
/// behind that feature, so nothing populates the map in a default build and
/// `store` has no caller there — while `lookup` is called unconditionally,
/// because a client can still send a `PickFileRef` to a build that cannot pick.
/// That case is now reported accurately ("no file picker result for this
/// session") instead of as an expiry that could not have happened.
#[cfg_attr(not(feature = "native-dialogs"), allow(dead_code))]
///
/// A backstop, not the primary bound — the TTL is. This exists so that a client
/// looping on `PickFile` cannot grow the map without limit inside one window.
pub const MAX_PICKED_FILES: usize = 64;

/// Why a `PickFileRef` could not be resolved. The two are different problems
/// with different fixes, and reporting both as "may have expired" sent users to
/// re-pick a file when nothing had expired.
#[derive(Debug, PartialEq, Eq)]
pub enum PickLookupFailure {
    /// No pick with that id was ever recorded on this session.
    Unknown,
    /// It was recorded, but longer ago than `PICKED_FILE_TTL`.
    Expired,
}

/// Store a pick, sweeping anything already past its TTL and enforcing the cap.
#[cfg_attr(not(feature = "native-dialogs"), allow(dead_code))]
pub fn store(
    picked_files: &mut HashMap<Uuid, PickedFileInfo>,
    request_id: Uuid,
    info: PickedFileInfo,
    now: Instant,
) {
    picked_files.retain(|_, existing| now.duration_since(existing.picked_at) < PICKED_FILE_TTL);
    picked_files.insert(request_id, info);

    // Oldest first, so the cap removes what is closest to expiring anyway.
    while picked_files.len() > MAX_PICKED_FILES {
        let oldest = picked_files
            .iter()
            .min_by_key(|(_, info)| info.picked_at)
            .map(|(id, _)| *id);
        match oldest {
            Some(id) => {
                picked_files.remove(&id);
            }
            None => break,
        }
    }
}

/// Whether this session actually picked `path`, and recently enough to use.
///
/// `FileSource::Path` used to be accepted verbatim: whatever absolute path
/// arrived on the socket was opened and sent to the peer, with no check that
/// the user had ever chosen it. The agent holds the ratchets, so the protocol
/// would then faithfully encrypt and deliver the caller's own files to a peer
/// the caller controls.
///
/// The reachable caller is script running in the allowlisted page -- a hostile
/// MDX document (the production CSP grants `unsafe-eval` so documents can
/// execute), an XSS, or a compromised dependency. `PickFile` even returns the
/// absolute path to the browser, so a page learns real paths to ask for.
///
/// The confinement is exactly the legitimate flow and costs it nothing: the UI
/// only ever sends a `Path` it received from `PickFile` on this same session
/// (`send-with-native-picker.ts` is its only producer). Anything else was never
/// a path this user chose.
///
/// Note what this means in the SHIPPED configuration. `native-dialogs` is off
/// by default and the Dockerfile does not enable it -- its own comment says
/// "Only enable when building for native desktop (not in Docker/server)" -- so
/// there is no picker, this set is permanently empty, and every `Path` is
/// refused. That is correct rather than a regression: a build with no picker
/// has no way to have legitimately produced a path, yet it was the one build
/// where the unvalidated branch was the ONLY one that worked, because
/// `PickFileRef` cannot resolve without a picker either.
///
/// Compared by exact `PathBuf` equality, deliberately. Canonicalising here
/// would touch the filesystem on a path the caller chose, and the value being
/// matched is one this process handed out, so it needs no normalising.
pub fn was_picked(
    picked_files: &HashMap<Uuid, PickedFileInfo>,
    path: &std::path::Path,
    now: Instant,
) -> bool {
    picked_files
        .values()
        .any(|info| info.file_path == path && now.duration_since(info.picked_at) < PICKED_FILE_TTL)
}

/// Resolve a pick, distinguishing "never seen" from "too old".
pub fn lookup<'a>(
    picked_files: &'a HashMap<Uuid, PickedFileInfo>,
    request_id: &Uuid,
    now: Instant,
) -> Result<&'a PickedFileInfo, PickLookupFailure> {
    match picked_files.get(request_id) {
        None => Err(PickLookupFailure::Unknown),
        Some(info) if now.duration_since(info.picked_at) >= PICKED_FILE_TTL => {
            Err(PickLookupFailure::Expired)
        }
        Some(info) => Ok(info),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn info(at: Instant) -> PickedFileInfo {
        PickedFileInfo {
            file_path: PathBuf::from("/tmp/f"),
            file_name: "f".to_string(),
            file_size: 1,
            picked_at: at,
        }
    }

    #[test]
    fn a_fresh_pick_resolves() {
        let mut map = HashMap::new();
        let now = Instant::now();
        let id = Uuid::new_v4();
        store(&mut map, id, info(now), now);
        assert!(lookup(&map, &id, now).is_ok());
    }

    #[test]
    fn a_pick_past_its_ttl_reports_expiry_rather_than_a_path() {
        let mut map = HashMap::new();
        let now = Instant::now();
        let id = Uuid::new_v4();
        map.insert(id, info(now - PICKED_FILE_TTL));
        assert_eq!(
            lookup(&map, &id, now).unwrap_err(),
            PickLookupFailure::Expired
        );
    }

    #[test]
    fn an_id_never_recorded_is_not_reported_as_expired() {
        // The whole point of the split: telling a user their pick expired when
        // it was never recorded sends them to re-pick a file, which will fail
        // the same way.
        let map = HashMap::new();
        assert_eq!(
            lookup(&map, &Uuid::new_v4(), Instant::now()).unwrap_err(),
            PickLookupFailure::Unknown
        );
    }

    #[test]
    fn a_path_this_session_picked_is_accepted() {
        let mut map = HashMap::new();
        let now = Instant::now();
        store(&mut map, Uuid::new_v4(), info(now), now);
        assert!(was_picked(&map, &PathBuf::from("/tmp/f"), now));
    }

    #[test]
    fn a_path_nobody_picked_is_refused() {
        // The whole point: an arbitrary absolute path arriving on the socket
        // is not a file the user chose, however well-formed it looks.
        let mut map = HashMap::new();
        let now = Instant::now();
        store(&mut map, Uuid::new_v4(), info(now), now);
        assert!(!was_picked(&map, &PathBuf::from("/etc/passwd"), now));
        assert!(!was_picked(&map, &PathBuf::from("/tmp/f2"), now));
    }

    #[test]
    fn a_pick_past_its_ttl_no_longer_authorises_its_path() {
        let mut map = HashMap::new();
        let now = Instant::now();
        map.insert(Uuid::new_v4(), info(now - PICKED_FILE_TTL));
        assert!(!was_picked(&map, &PathBuf::from("/tmp/f"), now));
    }

    #[test]
    fn an_empty_set_authorises_nothing() {
        // The shipped build has no picker, so this is its steady state. A
        // `.any()` over an empty map is false, but assert it rather than
        // trust it: the failure mode would be silent and total.
        let map = HashMap::new();
        assert!(!was_picked(&map, &PathBuf::from("/tmp/f"), Instant::now()));
    }

    #[test]
    fn storing_sweeps_what_has_already_expired() {
        let mut map = HashMap::new();
        let now = Instant::now();
        let stale = Uuid::new_v4();
        map.insert(stale, info(now - PICKED_FILE_TTL));
        store(&mut map, Uuid::new_v4(), info(now), now);
        assert!(!map.contains_key(&stale), "the map was insert-only before");
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn the_cap_bounds_a_burst_inside_one_window() {
        let mut map = HashMap::new();
        let now = Instant::now();
        // All fresh, so the TTL sweep removes nothing and only the cap can.
        for i in 0..(MAX_PICKED_FILES + 20) {
            let age = Duration::from_secs((MAX_PICKED_FILES + 20 - i) as u64);
            store(&mut map, Uuid::new_v4(), info(now - age), now);
        }
        assert_eq!(map.len(), MAX_PICKED_FILES);
    }

    #[test]
    fn the_cap_evicts_the_oldest_and_keeps_the_newest() {
        let mut map = HashMap::new();
        let now = Instant::now();
        let newest = Uuid::new_v4();
        for i in 0..MAX_PICKED_FILES {
            let age = Duration::from_secs((MAX_PICKED_FILES - i) as u64 + 10);
            store(&mut map, Uuid::new_v4(), info(now - age), now);
        }
        store(&mut map, newest, info(now), now);
        assert!(
            map.contains_key(&newest),
            "evicting the pick just made would be the one unusable outcome"
        );
        assert_eq!(map.len(), MAX_PICKED_FILES);
    }
}

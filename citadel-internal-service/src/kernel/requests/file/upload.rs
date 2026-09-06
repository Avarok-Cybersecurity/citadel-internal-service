use crate::kernel::requests::HandledRequestResult;
use crate::kernel::CitadelWorkspaceService;
use citadel_internal_service_connector::io_interface::IOInterface;
use citadel_internal_service_types::{
    FileSource, InternalServiceRequest, InternalServiceResponse, SendFileRequestFailure,
    SendFileRequestSuccess,
};
use citadel_sdk::logging::{error, info, warn};
use citadel_sdk::prelude::{
    NetworkError, NodeRequest, Ratchet, SendObject, TransferType, VirtualTargetType,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use uuid::Uuid;

/// Maximum accepted payload size for `FileSource::ByteContents`.
///
/// Caps the RAM a single request can demand before any disk I/O happens.
/// The constant is chosen to be reachable from real callers (i.e. the
/// guard actually fires) rather than a number large enough to be defeated
/// by transport-layer framing earlier in the stack:
///
///   * The WebSocket/JSON transport (used by the browser UI) encodes
///     payloads via `serde_json::to_string`, where a `Vec<u8>` expands
///     by roughly 3-4x. The resulting JSON must fit within the WS frame
///     limit, so the largest raw `data.len()` that survives serialization
///     is somewhere near 16 MiB.
///   * The TCP transport uses `bincode2` (binary framing via
///     `SerializingCodec` over a 64 MiB `LengthDelimitedCodec`) and does
///     not incur the JSON expansion, but the cap is applied uniformly so
///     behaviour does not depend on which transport happens to be in use.
///   * The browser-side workspace UI applies a much stricter cap (a few
///     MiB) before invoking this path.
///
/// 16 MiB therefore sits at the natural ceiling of the WebSocket framing
/// layer while still being multiple orders of magnitude above any sane
/// browser upload. Larger transfers must use the native `PickFile` flow,
/// which streams the file from disk and bypasses both this cap and the
/// JSON expansion entirely.
///
/// ## Not a pre-deserialization backstop
/// This guard fires in `handle()` *after* serde has already deserialized the
/// full `Vec<u8>` into RAM — it bounds what the SDK is asked to send, not the
/// transient memory serde allocates while decoding the frame. The actual
/// pre-deserialization defense is the transport frame limit (the bincode2
/// path uses a 64 MiB `LengthDelimitedCodec`; the WebSocket path is bounded
/// by its own frame cap). A client that streams a multi-GiB frame is stopped
/// by that codec limit, not by this constant.
const MAX_BYTE_CONTENTS_BYTES: usize = 16 * 1024 * 1024; // 16 MiB

/// Aggregate ceiling on bytes currently materialised under the shared
/// browser-transfer root. The per-request `MAX_BYTE_CONTENTS_BYTES` cap bounds
/// a single upload, but without an aggregate cap a client could fire many
/// concurrent uploads and accumulate `N × 16 MiB` on disk before the 10-minute
/// TTL cleanup fires. Before materialising, we sum the existing payloads and
/// reject the request if it would push the total past this bound. It is a
/// best-effort guard (a concurrent racer can briefly overshoot), not a hard
/// quota — the intent is to deny disk-exhaustion, not to meter to the byte.
/// 256 MiB ≈ 16 concurrent max-size uploads in flight.
const MAX_BROWSER_TRANSFER_TOTAL_BYTES: u64 = 256 * 1024 * 1024; // 256 MiB

/// Bytes of in-flight `ByteContents` uploads currently being written (between
/// the aggregate-cap reservation and the write completing). Added to the
/// on-disk total when enforcing `MAX_BROWSER_TRANSFER_TOTAL_BYTES` so that
/// concurrent uploads cannot all observe the same on-disk size and
/// collectively blow past the cap. Process-wide; resets to 0 on restart (the
/// startup sweep reclaims any crash leftovers).
static IN_FLIGHT_BROWSER_TRANSFER_BYTES: AtomicU64 = AtomicU64::new(0);

/// RAII guard that releases an in-flight byte reservation on drop, so the
/// reservation is returned even if the writing task panics/unwinds (not just
/// on the normal return paths). Created only AFTER a reservation is accepted.
struct InFlightReservation(u64);

impl Drop for InFlightReservation {
    fn drop(&mut self) {
        IN_FLIGHT_BROWSER_TRANSFER_BYTES.fetch_sub(self.0, Ordering::SeqCst);
    }
}

/// Subdirectory under `std::env::temp_dir()` where browser-uploaded payloads
/// are materialised. Each request gets its own UUID-named subdirectory
/// containing one file (preserving the user's filename), removed by a
/// delayed-cleanup task scheduled when the file is created.
const BROWSER_TRANSFER_SUBDIR: &str = "citadel-browser-transfers";

/// How long a `ByteContents` temp file persists before the cleanup task
/// removes it.
///
/// The lifetime must outlive the SDK's `process_outbound_file` call - which
/// runs *after* `remote.send(...).await` resolves in this handler, on the
/// SDK's main node loop after dequeuing the request. Once the SDK has
/// `File::open`'d the path, the file may be unlinked safely on POSIX (the
/// inode persists for the open FD until close).
///
/// 10 minutes is comfortably longer than any realistic dequeue + open
/// latency under saturation, and bounds disk-leak from a crash to that
/// window even without an external sweeper. The bound matters because the
/// 16 MiB per-file cap (`MAX_BYTE_CONTENTS_BYTES`) keeps the worst-case
/// leak bounded in absolute terms.
const TEMP_FILE_TTL: Duration = Duration::from_secs(600);

/// Windows device names reserved by the OS (case-insensitive, with or without
/// an extension). Creating a file with one of these stems triggers cryptic
/// failures on Windows; we prefix them so the same upload lands identically
/// on every platform the internal service runs on (CI covers windows-latest).
const WINDOWS_RESERVED_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Reduce a client-supplied file name to a safe single path component.
///
/// 1. Strip directory components ("photos/vacation.jpg" -> "vacation.jpg",
///    "../../../etc/passwd" -> "passwd") so the name can be joined onto the
///    per-request temp dir without escaping it.
/// 2. Drop ASCII control characters (incl. NUL), the characters Windows
///    forbids in file names (`< > : " / \ | ? *`), and deceptive Unicode
///    format characters (bidirectional overrides, zero-width marks, and the
///    BOM — the "Trojan Source" display-spoofing class). The first two
///    otherwise make `std::fs::write` fail cryptically or behave
///    inconsistently across platforms; stripping the last keeps the file name
///    a receiver displays consistent with its actual bytes (a name like
///    `report\u{202E}gpj.exe` must not render as `reportexe.jpg`). Only POSIX
///    printable basenames survive; the result is always plain text.
/// 3. Trim trailing dots/spaces (rejected by Windows) and prefix Windows
///    reserved device names. Falls back to "upload" when nothing usable
///    remains.
///
/// The original name is only ever used as the *basename* inside a UUID-named
/// per-request directory, so this is robustness/cross-platform hygiene rather
/// than the path-traversal defense (the UUID dir already provides that).
fn sanitize_file_name(raw: &str) -> String {
    // Reduce to the basename by splitting on BOTH separators regardless of the
    // host platform. `Path::file_name` only treats the *current* platform's
    // separator as one, so on a Unix service a browser-supplied Windows path
    // like `C:\fakepath\file.txt` (a common `<input type=file>` shape) would
    // stay a single component and later collapse to `Cfakepathfile.txt`.
    // Trailing separators are trimmed first so `a/b/` yields `b`, not "".
    let base = raw
        .trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("");

    let cleaned: String = base
        .chars()
        .filter(|c| {
            !c.is_control()
                && !is_deceptive_format_char(*c)
                && !matches!(c, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
        })
        .collect();

    // Windows rejects names with trailing dots or spaces.
    let trimmed = cleaned.trim().trim_end_matches('.').trim_end();
    if trimmed.is_empty() {
        return "upload".to_string();
    }

    // Cap the basename at NAME_MAX (255 bytes on ext4/APFS/NTFS) so a long
    // browser-supplied name fails closed by truncation rather than surfacing
    // a confusing ENAMETOOLONG `SendFileRequestFailure` on otherwise valid
    // input. Truncate on a UTF-8 char boundary so we never emit invalid text.
    const MAX_FILE_NAME_BYTES: usize = 255;
    let bounded = truncate_on_char_boundary(trimmed, MAX_FILE_NAME_BYTES);

    let stem = bounded.split('.').next().unwrap_or(bounded);
    if WINDOWS_RESERVED_NAMES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(stem))
    {
        return format!("_{bounded}");
    }

    bounded.to_string()
}

/// Unicode formatting characters that don't render as themselves and can be
/// used to spoof a file name's *apparent* name in a UI without changing the
/// path it actually resolves to. They are not path separators, so they pose no
/// traversal risk (the UUID request dir handles that), but a receiver that
/// displays the basename could be deceived: bidirectional overrides reorder
/// the visible glyphs ("Trojan Source") and zero-width / BOM characters hide
/// entirely. Stripping them keeps the displayed name faithful to the bytes.
fn is_deceptive_format_char(c: char) -> bool {
    matches!(c,
        // Bidirectional formatting: embeddings, overrides, isolates, marks.
        '\u{202A}'..='\u{202E}'   // LRE, RLE, PDF, LRO, RLO
        | '\u{2066}'..='\u{2069}' // LRI, RLI, FSI, PDI
        | '\u{200E}' | '\u{200F}' // LRM, RLM
        | '\u{061C}'              // Arabic letter mark
        // Zero-width / invisible separators and the byte-order mark.
        | '\u{200B}'..='\u{200D}' // ZWSP, ZWNJ, ZWJ
        | '\u{2060}'              // word joiner
        | '\u{FEFF}'              // zero-width no-break space / BOM
    )
}

/// Truncate `s` to at most `max_bytes` bytes without splitting a multi-byte
/// UTF-8 character (backs off to the nearest lower char boundary).
fn truncate_on_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Classification of the shared browser-transfer root prior to using it.
enum RootState {
    /// Does not exist yet — the normal first-boot state.
    Absent,
    /// A private (0700, no group/other access), real directory owned by us.
    Safe,
    /// Exists but is unsafe to use (symlink, non-directory, or any
    /// group/other permission bit). Carries a human-readable reason.
    Unsafe(String),
}

/// Inspect the shared browser-transfer root WITHOUT following symlinks or
/// creating anything. Mitigates CWE-377 (insecure temp file/dir): on a shared
/// `$TMPDIR`, a local attacker can pre-create the predictable root path as a
/// symlink (so our `remove_dir_all` sweep deletes through it, or our uploads
/// write through it) or as a non-private directory (so they can read the
/// unencrypted uploads, or swap a per-request subdir for a symlink between
/// materialise and the SDK's open).
///
/// We require the root to be STRICTLY PRIVATE — `0700`, no group/other bits at
/// all. Merely "not writable by others" (`0755`) is insufficient: browser
/// uploads are written as plaintext under this root, so a world-*readable*
/// root would expose them to other local users. We only ever use a root we
/// created `0700` ourselves; anything else is rejected so the service fails
/// closed rather than writing secrets somewhere readable.
fn classify_root(root: &Path) -> RootState {
    let meta = match std::fs::symlink_metadata(root) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return RootState::Absent,
        Err(e) => return RootState::Unsafe(format!("cannot stat {root:?}: {e}")),
    };
    if meta.file_type().is_symlink() {
        return RootState::Unsafe(format!("{root:?} is a symlink"));
    }
    if !meta.is_dir() {
        return RootState::Unsafe(format!("{root:?} exists but is not a directory"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        // Reject anything we don't own. A local attacker could pre-create the
        // predictable root path as their OWN 0700 directory — the permission
        // check alone would accept it, then they could tamper with the
        // per-request subdirs inside. Only a root owned by our effective uid
        // is trustworthy.
        let our_euid = unsafe { libc::geteuid() };
        if meta.uid() != our_euid {
            return RootState::Unsafe(format!(
                "{root:?} is owned by uid {} but we are uid {our_euid}; refusing",
                meta.uid()
            ));
        }
        let mode = meta.permissions().mode();
        // Reject ANY group/other permission bit — the root must be 0700 so
        // uploads under it are neither readable nor tamperable by other users.
        if mode & 0o077 != 0 {
            return RootState::Unsafe(format!(
                "{root:?} is not private (mode {:o}); expected 0700",
                mode & 0o7777
            ));
        }
    }
    RootState::Safe
}

/// Ensure the shared browser-transfer root exists as a private directory we
/// own, returning an error if it exists but is unsafe.
///
/// The `Absent` branch creates the root with a NON-recursive `0700` create so
/// the operation fails (rather than silently succeeding) if a symlink or
/// directory was planted at the path between `classify_root` and here — the
/// classic create-time TOCTOU. A benign lost race (two concurrent first
/// uploads) surfaces as `AlreadyExists`, which we resolve by re-classifying:
/// proceed only if the winner created a private root, else fail closed.
fn ensure_private_root(root: &Path) -> std::io::Result<()> {
    match classify_root(root) {
        RootState::Safe => Ok(()),
        RootState::Unsafe(reason) => Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("refusing insecure browser-transfer root: {reason}"),
        )),
        RootState::Absent => match create_private_dir_exclusive(root) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => match classify_root(root) {
                RootState::Safe => Ok(()),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("browser-transfer root {root:?} appeared and is not private"),
                )),
            },
            Err(e) => Err(e),
        },
    }
}

/// Create a single directory with `0700` perms, failing if it already exists
/// (non-recursive; the parent `$TMPDIR` is assumed present). The exclusivity
/// is what makes root creation symlink-plant resistant.
fn create_private_dir_exclusive(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(dir)
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir(dir)
    }
}

/// Write `data` to `path` with private `0600` perms so the materialised
/// upload is not readable by other local users even if the enclosing dirs
/// were somehow loosened. On non-Unix we fall back to `std::fs::write`.
fn write_private_file(path: &Path, data: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(data)
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, data)
    }
}

/// Sum the bytes of all payload files currently materialised under `root`
/// (one file per per-request subdir). Best-effort: unreadable entries are
/// skipped rather than failing the caller. Used to enforce the aggregate
/// disk cap before a new materialisation.
fn browser_transfer_root_bytes(root: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(sub) = std::fs::read_dir(entry.path()) else {
            continue;
        };
        for file in sub.flatten() {
            if let Ok(meta) = file.metadata() {
                if meta.is_file() {
                    total = total.saturating_add(meta.len());
                }
            }
        }
    }
    total
}

/// One-shot, best-effort sweep of the shared browser-transfer temp root.
/// Removes any per-request subdirectory older than `TEMP_FILE_TTL` so the
/// service doesn't accumulate orphaned payloads across crash-restart
/// cycles. The per-request `schedule_temp_dir_cleanup` task is cancelled
/// when the process exits, so without this every crash before the 10-min
/// timer fires leaks the materialised file; over many crashes the leak
/// is unbounded in count. The 16 MiB per-file cap bounds per-file size
/// but not aggregate disk usage.
///
/// Intended to run once at binary startup. The function uses blocking
/// `std::fs` intentionally so it does not require a tokio runtime — the
/// current call site at `service/src/main.rs` runs inside
/// `#[tokio::main]` but before any other tasks are spawned, so the
/// blocking I/O does not park a worker that another task is waiting
/// on. Errors are logged but not propagated — a sweep failure must
/// not block service startup.
pub fn sweep_stale_browser_transfers() {
    sweep_browser_transfers_in(
        &std::env::temp_dir().join(BROWSER_TRANSFER_SUBDIR),
        TEMP_FILE_TTL,
    );
}

/// Inner sweep that operates on an arbitrary root path. Split out so
/// tests can pass an isolated per-test directory rather than sharing
/// the production `$TMPDIR/citadel-browser-transfers/` root with
/// parallel test threads. Production callers go through
/// `sweep_stale_browser_transfers`.
fn sweep_browser_transfers_in(root: &Path, max_age: Duration) {
    // Refuse to sweep through a symlinked or world-writable root: a
    // `remove_dir_all` walk rooted at an attacker-controlled path could
    // delete unrelated files. The root only exists after the first upload,
    // so absent is the normal first-boot state and not worth logging.
    match classify_root(root) {
        RootState::Absent => return,
        RootState::Safe => {}
        RootState::Unsafe(reason) => {
            warn!(
                target: "citadel",
                "Browser-transfer sweep: refusing insecure root ({reason}); skipping"
            );
            return;
        }
    }
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        // Already handled NotFound via classify_root, but a TOCTOU removal
        // between the two calls is benign — nothing to sweep.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            warn!(
                target: "citadel",
                "Browser-transfer sweep: failed to read {:?}: {}", root, e
            );
            return;
        }
    };
    let mut removed = 0usize;
    for entry in entries.flatten() {
        // `modified()` is unsupported on a handful of exotic filesystems;
        // when unavailable, fall back to deleting the entry on the
        // assumption that "no timestamp" means "older than we can
        // measure", which is safe for stale-cleanup purposes.
        let is_stale = entry
            .metadata()
            .and_then(|m| m.modified())
            .map(|t| t.elapsed().unwrap_or_default() > max_age)
            .unwrap_or(true);
        if !is_stale {
            continue;
        }
        let path = entry.path();
        match std::fs::remove_dir_all(&path) {
            Ok(()) => removed += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => warn!(
                target: "citadel",
                "Browser-transfer sweep: failed to remove {:?}: {}", path, e
            ),
        }
    }
    if removed > 0 {
        info!(
            target: "citadel",
            "Browser-transfer sweep removed {} stale request dir(s) under {:?}",
            removed, root
        );
    }
}

/// Schedule a best-effort delayed cleanup of a per-request temp directory
/// (containing a single materialised payload file).
///
/// We do NOT unlink at handler exit (the previous Drop-guard approach):
/// `remote.send(req).await` resolves when the request is accepted by the
/// SDK's mpsc channel, which is *before* the SDK's node loop has dequeued
/// the request and called `File::open(&path)`. An immediate unlink would
/// race the SDK's open, causing intermittent ENOENT failures whose only
/// observable symptom is a silent transfer drop.
///
/// Spawning a long-delay cleanup decouples deletion from the handler
/// lifetime entirely. By the time the delay elapses, the SDK has either
/// (a) opened the file and now holds an FD that survives unlink on POSIX,
/// or (b) failed to consume the request inside 10 minutes - in which case
/// the transfer was already dead and reclaiming the disk is the correct
/// action.
///
/// We remove the per-request directory rather than just the file so a
/// stray subdirectory doesn't leak even on partial cleanup failure.
fn schedule_temp_dir_cleanup(dir: PathBuf) {
    tokio::spawn(async move {
        tokio::time::sleep(TEMP_FILE_TTL).await;
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => {
                info!(target: "citadel", "Cleaned up browser temp dir {:?}", dir);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Already gone (operator sweep, prior cleanup) - fine.
            }
            Err(e) => {
                warn!(
                    target: "citadel",
                    "Failed to clean up browser temp dir {:?}: {}",
                    dir, e
                );
            }
        }
    });
}

/// Materialise raw byte contents into a one-shot temp file and schedule its
/// cleanup. Returns the path for handing to the SDK.
///
/// All disk I/O happens inside `spawn_blocking` so the tokio worker servicing
/// this handler is not parked on a blocking write. Cleanup is scheduled via
/// `schedule_temp_dir_cleanup` *after* the write attempt completes (success
/// OR failure), so partial states like "directory created but write failed"
/// do not leak the empty subdir.
async fn materialize_byte_contents(
    file_name: &str,
    data: Vec<u8>,
) -> Result<PathBuf, NetworkError> {
    materialize_byte_contents_in(
        &std::env::temp_dir().join(BROWSER_TRANSFER_SUBDIR),
        file_name,
        data,
    )
    .await
}

/// Inner materialise that writes under an arbitrary `root`. Split out (like
/// `sweep_browser_transfers_in`) so tests can use an isolated, private
/// per-test root instead of the shared production
/// `$TMPDIR/citadel-browser-transfers/` — which other processes (or a stale
/// pre-upgrade directory) may have left at non-private perms.
async fn materialize_byte_contents_in(
    root: &Path,
    file_name: &str,
    data: Vec<u8>,
) -> Result<PathBuf, NetworkError> {
    let safe_name = sanitize_file_name(file_name);

    // Each request gets its own UUID-named subdirectory under the shared
    // browser-transfer root, with the user-provided file name preserved
    // inside. The SDK reads the *basename* of the path as the transfer's
    // visible filename, so the receiver sees the original `file_name`
    // rather than a UUID-mangled stem. Per-request isolation also means
    // two simultaneous uploads with the same name cannot collide.
    let request_dir = root.join(Uuid::new_v4().to_string());
    let temp_path = request_dir.join(&safe_name);

    let root_for_blocking = root.to_path_buf();
    let request_dir_for_blocking = request_dir.clone();
    let temp_path_for_blocking = temp_path.clone();
    let bytes_len = data.len();

    let write_result = tokio::task::spawn_blocking(move || {
        // Secure the shared root, then create the per-request subdir (0700)
        // and write the payload (0600) so the materialised upload is private
        // end to end (see classify_root / ensure_private_root).
        ensure_private_root(&root_for_blocking)?;

        // Aggregate disk-exhaustion guard. The total counted is the bytes
        // already ON DISK plus the bytes of every upload currently being
        // written (the in-flight reservation). Reserving atomically BEFORE
        // reading the on-disk total closes the concurrent-bypass race where
        // many uploads observe the same on-disk size and all pass the check:
        // a later racer sees the earlier racers' reservations included in
        // `in_flight_before` and is rejected if there's no room.
        let reserve = data.len() as u64;
        let in_flight_before =
            IN_FLIGHT_BROWSER_TRANSFER_BYTES.fetch_add(reserve, Ordering::SeqCst);
        // RAII release so the reservation is given back on EVERY exit from
        // here on — normal return, early error, or a panic that unwinds the
        // spawn_blocking task (which surfaces as a JoinError the closure never
        // gets to clean up). Without this a panicked upload would leak its
        // reservation forever, permanently shrinking the effective cap.
        let _reservation = InFlightReservation(reserve);
        let on_disk = browser_transfer_root_bytes(&root_for_blocking);
        if on_disk
            .saturating_add(in_flight_before)
            .saturating_add(reserve)
            > MAX_BROWSER_TRANSFER_TOTAL_BYTES
        {
            // `_reservation` drops here and releases `reserve`.
            return Err(std::io::Error::other(format!(
                "browser-transfer root holds {on_disk} on disk + {in_flight_before} in flight; \
                 this {reserve} byte upload would exceed the \
                 {MAX_BROWSER_TRANSFER_TOTAL_BYTES} byte aggregate cap"
            )));
        }

        // On success the payload is now on disk (counted by
        // browser_transfer_root_bytes); on failure it was never written.
        // Either way `_reservation` releases the in-flight bytes on drop.
        //
        // Use the EXCLUSIVE (non-recursive) create for the per-request dir:
        // the root is guaranteed to exist (ensure_private_root above), so we
        // never need to create through it, and an exclusive create fails
        // closed rather than writing through anything pre-planted at the
        // (random) request path.
        create_private_dir_exclusive(&request_dir_for_blocking)
            .and_then(|()| write_private_file(&temp_path_for_blocking, &data))
    })
    .await;

    match write_result {
        Ok(Ok(())) => {
            info!(
                target: "citadel",
                "Wrote browser file {:?} ({} bytes) to {:?}",
                safe_name, bytes_len, temp_path
            );
            // Schedule cleanup of the *directory* rather than the file
            // alone, so the request dir doesn't outlive the file.
            schedule_temp_dir_cleanup(request_dir);
            Ok(temp_path)
        }
        Ok(Err(e)) => {
            // The directory may or may not exist depending on which step
            // failed (create_dir_all vs write). Schedule cleanup either
            // way so an empty subdir from a partial-success state cannot
            // leak; remove_dir_all tolerates missing paths.
            schedule_temp_dir_cleanup(request_dir);
            Err(NetworkError::msg(format!(
                "Failed to write browser file to temp: {e}"
            )))
        }
        Err(join_err) => {
            // spawn_blocking JoinError - the closure didn't run to
            // completion (panic or runtime cancellation). The directory
            // may have been partially constructed. Best-effort cleanup.
            schedule_temp_dir_cleanup(request_dir);
            Err(NetworkError::msg(format!(
                "Failed to run blocking temp-file write: {join_err}"
            )))
        }
    }
}

pub async fn handle<T: IOInterface, R: Ratchet>(
    this: &CitadelWorkspaceService<T, R>,
    uuid: Uuid,
    request: InternalServiceRequest,
) -> Option<HandledRequestResult> {
    let InternalServiceRequest::SendFile {
        request_id,
        source,
        cid,
        peer_cid,
        chunk_size,
        transfer_type,
    } = request
    else {
        unreachable!("Should never happen if programmed properly")
    };
    let remote = this.remote().clone();

    // Validate the target session (and peer, if any) BEFORE resolving the
    // source. Materialising a `ByteContents` payload writes up to 16 MiB to
    // disk and keeps it for the cleanup TTL; doing that for a bogus `cid`
    // would let any local client spam the service into a disk-exhaustion DoS
    // that only fails *after* the write. This is a cheap early reject — the
    // `send_request` builder below re-checks under its own lock, which also
    // covers the rare disconnect-between-validation-and-build TOCTOU.
    {
        let lock = this.server_connection_map.read();
        let rejection: Option<&str> = match lock.get(&cid) {
            None => Some("upload: Server Connection Not Found"),
            Some(conn) => match peer_cid {
                Some(pc) if !conn.peers.contains_key(&pc) => Some("Peer Connection Not Found"),
                _ => None,
            },
        };
        drop(lock);
        if let Some(message) = rejection {
            return Some(HandledRequestResult {
                response: InternalServiceResponse::SendFileRequestFailure(SendFileRequestFailure {
                    cid,
                    message: message.to_string(),
                    request_id: Some(request_id),
                }),
                uuid,
            });
        }
    }

    // Resolve the source to a filesystem path. For ByteContents, this also
    // schedules the temp-file cleanup (see `materialize_byte_contents`).
    //
    // PickFileRef is the only branch that needs the connection-map lock
    // here; ByteContents materialisation deliberately does its disk I/O
    // OUTSIDE any lock so concurrent connection-map writers are not
    // stalled by the spawn_blocking write.
    let resolved_path: Result<PathBuf, NetworkError> = match source {
        // Confined to paths THIS session picked. Accepting the path verbatim
        // meant whatever absolute path arrived on the socket was opened and
        // sent to the peer: the agent holds the ratchets, so the protocol
        // would faithfully encrypt and deliver the caller's own files to a
        // peer the caller controls. Script in the allowlisted page is the
        // reachable caller, and `PickFile` hands the browser real absolute
        // paths to ask for.
        //
        // This is exactly the legitimate flow and costs it nothing: the UI
        // only ever sends back a `Path` it received from `PickFile` on this
        // same session.
        FileSource::Path(path) => {
            // A caller that can already read the filesystem -- a native CLI or
            // desktop client over TCP -- gains nothing from being refused, and
            // refusing it would break that contract for no security. A browser
            // caller cannot read files at all, so for it an accepted path is a
            // real escalation. See
            // IOInterface::CALLER_CAN_ALREADY_READ_LOCAL_FILES.
            let picked = T::CALLER_CAN_ALREADY_READ_LOCAL_FILES || {
                let lock = this.server_connection_map.read();
                let ok = match lock.get(&cid) {
                    Some(conn) => crate::kernel::picked_files::was_picked(
                        &conn.picked_files,
                        &path,
                        std::time::Instant::now(),
                    ),
                    None => false,
                };
                drop(lock);
                ok
            };
            if picked {
                Ok(path)
            } else {
                // Does not echo the path back. The caller supplied it, so
                // repeating it teaches them nothing they did not know, and a
                // rejected path is exactly the kind of value that should not
                // be written into a log a probe can read.
                Err(NetworkError::msg(
                    "SendFile source must be a file chosen through this session's \
                     file picker. Use PickFile first and send the resulting \
                     PickFileRef, or send the bytes as ByteContents.",
                ))
            }
        }
        FileSource::PickFileRef {
            pick_file_request_id,
        } => {
            let lock = this.server_connection_map.read();
            match lock.get(&cid) {
                Some(conn) => {
                    // The two failures are different problems with different
                    // fixes. This reported both as "may have expired" -- and
                    // nothing expired anything, so the only diagnosis the user
                    // ever got named a cause that could not occur, and sent
                    // them to re-open a picker that would fail the same way.
                    match crate::kernel::picked_files::lookup(
                        &conn.picked_files,
                        &pick_file_request_id,
                        std::time::Instant::now(),
                    ) {
                        Ok(picked_info) => {
                            info!(target: "citadel", "Resolved PickFileRef {:?} to path {:?}",
                                pick_file_request_id, picked_info.file_path);
                            Ok(picked_info.file_path.clone())
                        }
                        Err(crate::kernel::picked_files::PickLookupFailure::Expired) => {
                            Err(NetworkError::msg(format!(
                                "The file picked for {:?} has expired; pick it again.",
                                pick_file_request_id
                            )))
                        }
                        Err(crate::kernel::picked_files::PickLookupFailure::Unknown) => {
                            Err(NetworkError::msg(format!(
                                "No file picker result for {:?} on this session.                                  It belongs to a different session, or the agent restarted.",
                                pick_file_request_id
                            )))
                        }
                    }
                }
                None => Err(NetworkError::msg(
                    "Connection not found for PickFileRef lookup",
                )),
            }
        }
        FileSource::ByteContents { file_name, data } => {
            // Size guard: fail fast before any I/O.
            if data.len() > MAX_BYTE_CONTENTS_BYTES {
                Err(NetworkError::msg(format!(
                    "ByteContents payload of {} bytes exceeds the {} byte maximum",
                    data.len(),
                    MAX_BYTE_CONTENTS_BYTES
                )))
            } else {
                materialize_byte_contents(&file_name, data).await
            }
        }
    };

    // A REVFS push needs its completion reported back to the requesting
    // browser: `SendFileRequestSuccess` below only means "queued", so the
    // browser waits for the Sender-side TransferComplete tick — which must
    // carry this request_id. `SendObject` has no field to thread it through,
    // so it is registered here and consumed when the Sender handle arrives
    // (responses/object_transfer_handle.rs). The Sender-handle scope key is
    // the peer's cid for P2P, and — because a c2s Sender handle carries
    // source == receiver == session_cid — the session's own cid for c2s.
    let is_revfs_push = matches!(
        transfer_type,
        TransferType::RemoteEncryptedVirtualFilesystem { .. }
    );
    let correlation_scope = peer_cid.unwrap_or(cid);

    // Build the NodeRequest under a brief lock, then drop the lock
    // before any await (the SDK `remote.send` below is async and must not
    // happen while the RwLock is held). Write lock: REVFS pushes also
    // register their tick correlation here (see above).
    let send_request: Result<NodeRequest, NetworkError> = match resolved_path {
        Ok(file_path) => {
            let mut lock = this.server_connection_map.write();
            match lock.get_mut(&cid) {
                Some(conn) => {
                    if let Some(peer_cid) = peer_cid {
                        if conn.peers.contains_key(&peer_cid) {
                            // Use the deterministic `LocalGroupPeer { session_cid,
                            // peer_cid }` value the SDK expects, instead of
                            // requiring `peer_conn.remote.user()`. The previous
                            // implementation gated on `peer_conn.remote` being
                            // `Some`, which is only true on the side that called
                            // `connect_to_peer_custom`. For acceptor-side
                            // connections `remote` is `None` and the request
                            // failed with "Peer connection missing remote
                            // (acceptor-only connection cannot send files)" —
                            // which silently broke file transfer in either
                            // direction whenever the sender wasn't also the
                            // P2P initiator. Same shape we already use for
                            // messaging via the sink works for SendObject too.
                            if is_revfs_push {
                                conn.revfs_correlations
                                    .register_push(correlation_scope, request_id);
                            }
                            Ok(NodeRequest::SendObject(SendObject {
                                source: Box::new(file_path),
                                chunk_size,
                                session_cid: cid,
                                v_conn_type: VirtualTargetType::LocalGroupPeer {
                                    session_cid: cid,
                                    peer_cid,
                                },
                                transfer_type,
                            }))
                        } else {
                            Err(NetworkError::msg("Peer Connection Not Found"))
                        }
                    } else {
                        if is_revfs_push {
                            conn.revfs_correlations
                                .register_push(correlation_scope, request_id);
                        }
                        Ok(NodeRequest::SendObject(SendObject {
                            source: Box::new(file_path),
                            chunk_size,
                            session_cid: cid,
                            v_conn_type: VirtualTargetType::LocalGroupServer { session_cid: cid },
                            transfer_type,
                        }))
                    }
                }
                None => {
                    error!(target: "citadel","upload: server connection not found");
                    Err(NetworkError::msg("upload: Server Connection Not Found"))
                }
            }
        }
        Err(e) => Err(e),
    }; // Lock dropped here - BEFORE any await

    match send_request {
        Ok(request) => {
            let result = remote.send(request).await;
            match result {
                Ok(_) => {
                    info!(target: "citadel","InternalServiceRequest Send File Success");
                    let response =
                        InternalServiceResponse::SendFileRequestSuccess(SendFileRequestSuccess {
                            cid,
                            request_id: Some(request_id),
                        });
                    Some(HandledRequestResult { response, uuid })
                }
                Err(err) => {
                    error!(target: "citadel","InternalServiceRequest Send File Failure");
                    // The push never went out — its correlation entry must
                    // not sit in the FIFO and claim the NEXT push's ticks.
                    if is_revfs_push {
                        if let Some(conn) = this.server_connection_map.write().get_mut(&cid) {
                            conn.revfs_correlations
                                .cancel_push(correlation_scope, request_id);
                        }
                    }
                    let response =
                        InternalServiceResponse::SendFileRequestFailure(SendFileRequestFailure {
                            cid,
                            message: err.into_string(),
                            request_id: Some(request_id),
                        });
                    Some(HandledRequestResult { response, uuid })
                }
            }
        }
        Err(err) => {
            let response =
                InternalServiceResponse::SendFileRequestFailure(SendFileRequestFailure {
                    cid,
                    message: err.into_string(),
                    request_id: Some(request_id),
                });
            Some(HandledRequestResult { response, uuid })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{materialize_byte_contents_in, sanitize_file_name};

    #[test]
    fn sanitize_strips_path_components() {
        assert_eq!(sanitize_file_name("photos/vacation.jpg"), "vacation.jpg");
        assert_eq!(sanitize_file_name("../../../etc/passwd"), "passwd");
        assert_eq!(sanitize_file_name("plain.txt"), "plain.txt");
    }

    #[test]
    fn sanitize_handles_windows_and_fakepath_separators() {
        // Browser `<input type=file>` fakepath, on a Unix host: must reduce to
        // the real basename, not collapse to "Cfakepathfile.txt".
        assert_eq!(sanitize_file_name("C:\\fakepath\\file.txt"), "file.txt");
        assert_eq!(sanitize_file_name("photos\\trip\\pic.png"), "pic.png");
        // Trailing backslash still yields the last real component.
        assert_eq!(sanitize_file_name("dir\\sub\\"), "sub");
        // Reserved-name handling still applies after the split.
        assert_eq!(sanitize_file_name("C:\\fakepath\\CON.txt"), "_CON.txt");
    }

    #[test]
    fn sanitize_handles_pathological_input() {
        assert_eq!(sanitize_file_name(""), "upload");
        assert_eq!(sanitize_file_name("/"), "upload");
        // A pure directory-style name has no file component
        assert_eq!(sanitize_file_name("a/b/"), "b");
    }

    #[test]
    fn sanitize_strips_control_and_forbidden_chars() {
        // NUL and other ASCII control characters are removed.
        assert_eq!(sanitize_file_name("a\u{0}b\u{7}.txt"), "ab.txt");
        // Windows-forbidden characters are removed (the colon, pipe, etc.).
        assert_eq!(sanitize_file_name("a<b>c:d|e?.txt"), "abcde.txt");
        // A name that becomes empty after stripping falls back to "upload".
        assert_eq!(sanitize_file_name("\u{0}\u{1}\u{2}"), "upload");
        // Trailing dots/spaces (rejected by Windows) are trimmed.
        assert_eq!(sanitize_file_name("report.  "), "report");
    }

    #[test]
    fn sanitize_strips_deceptive_unicode_format_chars() {
        // Right-to-left override ("Trojan Source"): the raw name visually
        // renders as "reportexe.jpg" but the bytes spell "report<RLO>gpj.exe".
        // After stripping the override the displayed name matches the bytes.
        assert_eq!(sanitize_file_name("report\u{202E}gpj.exe"), "reportgpj.exe");
        // Zero-width space and BOM are invisible padding — removed entirely.
        assert_eq!(sanitize_file_name("a\u{200B}b\u{FEFF}.txt"), "ab.txt");
        // Bidi isolates and the word joiner are stripped too.
        assert_eq!(sanitize_file_name("x\u{2066}y\u{2060}z.bin"), "xyz.bin");
        // A name made entirely of deceptive format chars collapses to the
        // fallback rather than an empty (and unwritable) name.
        assert_eq!(sanitize_file_name("\u{202E}\u{200B}\u{FEFF}"), "upload");
    }

    #[test]
    fn sanitize_guards_windows_reserved_names() {
        assert_eq!(sanitize_file_name("CON"), "_CON");
        assert_eq!(sanitize_file_name("con.txt"), "_con.txt");
        assert_eq!(sanitize_file_name("LPT1.log"), "_LPT1.log");
        // Only an exact reserved stem is guarded; a longer name is untouched.
        assert_eq!(sanitize_file_name("CONSOLE.txt"), "CONSOLE.txt");
        assert_eq!(sanitize_file_name("com10"), "com10");
    }

    #[test]
    fn classify_root_flags_symlinks_and_accepts_private_dirs() {
        use super::{classify_root, RootState};

        let (base, _guard) = isolated_sweep_root("classify");
        std::fs::create_dir_all(&base).expect("create classify base");

        // Absent path.
        let absent = base.join("does-not-exist");
        assert!(matches!(classify_root(&absent), RootState::Absent));

        // A real private directory is Safe.
        let real = base.join("real");
        std::fs::create_dir_all(&real).expect("create real dir");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700))
                .expect("chmod 0700");
        }
        assert!(matches!(classify_root(&real), RootState::Safe));

        // A symlink (even pointing at a valid dir) is rejected.
        #[cfg(unix)]
        {
            let link = base.join("link");
            std::os::unix::fs::symlink(&real, &link).expect("create symlink");
            assert!(matches!(classify_root(&link), RootState::Unsafe(_)));
        }
    }

    #[cfg(unix)]
    #[test]
    fn classify_root_rejects_nonprivate_dir() {
        use super::{classify_root, RootState};
        use std::os::unix::fs::PermissionsExt;

        let (base, _guard) = isolated_sweep_root("classify_perms");
        std::fs::create_dir_all(&base).expect("create perms base");

        // Every non-private mode is rejected: world-writable (tamper) AND
        // merely group/other-READABLE (would expose plaintext uploads).
        for mode in [0o777, 0o755, 0o750, 0o705, 0o770] {
            let dir = base.join(format!("mode_{mode:o}"));
            std::fs::create_dir_all(&dir).expect("create dir");
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode)).expect("chmod");
            assert!(
                matches!(classify_root(&dir), RootState::Unsafe(_)),
                "mode {mode:o} should be rejected as non-private"
            );
        }

        // 0700 is the only accepted mode.
        let private = base.join("private");
        std::fs::create_dir_all(&private).expect("create private dir");
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).expect("chmod");
        assert!(matches!(classify_root(&private), RootState::Safe));
    }

    /// A symlink planted at the root path between classify and create must
    /// not be traversed: `ensure_private_root` uses an exclusive create, so
    /// it fails closed rather than writing through the attacker's symlink.
    #[cfg(unix)]
    #[test]
    fn ensure_private_root_refuses_planted_symlink() {
        use super::ensure_private_root;

        let (base, _guard) = isolated_sweep_root("ensure_symlink");
        std::fs::create_dir_all(&base).expect("create base");
        let target = base.join("target");
        std::fs::create_dir_all(&target).expect("create target");

        // Attacker plants a symlink where the root would be created.
        let root = base.join("root");
        std::os::unix::fs::symlink(&target, &root).expect("plant symlink");

        let err = ensure_private_root(&root).expect_err("must refuse symlinked root");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    /// A symlinked sweep root must be left untouched — the sweep must not
    /// follow it and delete through to the symlink target.
    #[cfg(unix)]
    #[test]
    fn sweep_refuses_symlinked_root() {
        use super::sweep_browser_transfers_in;
        use std::time::Duration;

        let (base, _guard) = isolated_sweep_root("sweep_symlink");
        std::fs::create_dir_all(&base).expect("create base");

        // `target` stands in for an unrelated directory the attacker wants
        // deleted; it contains a stale entry the sweep would otherwise remove.
        let target = base.join("target");
        let victim = target.join("00000000-0000-0000-0000-000000000000");
        std::fs::create_dir_all(&victim).expect("create victim dir");

        let link = base.join("link");
        std::os::unix::fs::symlink(&target, &link).expect("create symlink root");

        // Sweep the symlinked root with ZERO max-age (everything is "stale").
        sweep_browser_transfers_in(&link, Duration::ZERO);

        assert!(
            victim.exists(),
            "sweep followed a symlinked root and deleted through it"
        );
    }

    /// Exercises the IO-error branch of `materialize_byte_contents`. We point
    /// the root at a regular FILE so securing/creating the per-request dir
    /// under it fails (root "is not a directory"). The error flows through the
    /// closure's `?` into the `Ok(Err(e))` arm, which wraps it as "Failed to
    /// write browser file to temp" and schedules cleanup.
    ///
    /// (The previous overlong-filename trigger no longer errors now that
    /// `sanitize_file_name` truncates names to NAME_MAX — see
    /// `sanitize_truncates_overlong_names`.)
    #[tokio::test]
    async fn materialize_returns_err_when_write_fails() {
        let (base, _guard) = isolated_sweep_root("materialize_err");
        std::fs::create_dir_all(&base).expect("create base");
        let root_as_file = base.join("not-a-dir");
        std::fs::write(&root_as_file, b"x").expect("create regular file at root path");

        let result = materialize_byte_contents_in(&root_as_file, "file.bin", vec![1, 2, 3]).await;
        assert!(
            result.is_err(),
            "expected IO error when root is a file, got Ok({:?})",
            result.ok()
        );
        let msg = result.unwrap_err().into_string();
        assert!(
            msg.contains("Failed to write browser file to temp"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn sanitize_truncates_overlong_names() {
        // A name well past NAME_MAX is truncated to <= 255 bytes (no
        // ENAMETOOLONG downstream) while staying valid UTF-8.
        let long = "a".repeat(300);
        let out = sanitize_file_name(&long);
        assert!(out.len() <= 255, "expected <=255 bytes, got {}", out.len());
        assert!(out.chars().all(|c| c == 'a'));

        // Multi-byte chars are never split mid-codepoint.
        let multibyte = "é".repeat(200); // 400 bytes
        let out = sanitize_file_name(&multibyte);
        assert!(out.len() <= 255);
        assert!(std::str::from_utf8(out.as_bytes()).is_ok());
        assert!(out.chars().all(|c| c == 'é'));
    }

    /// Happy-path counterpart to `materialize_returns_err_when_write_fails`.
    /// A reasonable filename and small payload must produce a path under the
    /// given root, with the file actually present on disk, containing the
    /// bytes we asked for, and written with private 0600 perms.
    #[tokio::test]
    async fn materialize_writes_payload_to_temp_path() {
        let (root, _guard) = isolated_private_root("materialize_ok");
        let path = materialize_byte_contents_in(&root, "hello.bin", vec![0xDE, 0xAD, 0xBE, 0xEF])
            .await
            .expect("materialize should succeed");

        // Path lives under the root we provided.
        assert!(
            path.starts_with(&root),
            "path {path:?} not under root {root:?}"
        );
        assert_eq!(path.file_name().and_then(|n| n.to_str()), Some("hello.bin"));

        // File on disk has exactly the bytes we wrote.
        let read_back = std::fs::read(&path).expect("read written temp file");
        assert_eq!(read_back, vec![0xDE, 0xAD, 0xBE, 0xEF]);

        // The payload file is private (0600) and its parent dir is 0700.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let file_mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(file_mode, 0o600, "payload file should be 0600");
            let dir_mode = std::fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(dir_mode, 0o700, "request dir should be 0700");
        }

        // Best-effort eager cleanup so the test doesn't lean on the
        // 10-minute deferred TTL. Cleanup of the parent dir is what
        // production relies on; we replicate that here.
        if let Some(parent) = path.parent() {
            let _ = std::fs::remove_dir_all(parent);
        }
    }

    /// Helper: build an isolated sweep root under `$TMPDIR` so each
    /// test owns its directory and parallel cargo-test threads can't
    /// step on each other. Returns `(root_path, _cleanup_guard)` —
    /// when the guard drops the directory is best-effort removed.
    fn isolated_sweep_root(label: &str) -> (std::path::PathBuf, IsolatedRootGuard) {
        let root = std::env::temp_dir().join(format!(
            "citadel-browser-transfers-test-{}-{}",
            label,
            uuid::Uuid::new_v4()
        ));
        (root.clone(), IsolatedRootGuard(root))
    }

    /// Like `isolated_sweep_root` but eagerly creates the root as a private
    /// `0700` directory, matching what production creates — required now that
    /// `classify_root` rejects any non-private root.
    fn isolated_private_root(label: &str) -> (std::path::PathBuf, IsolatedRootGuard) {
        let (root, guard) = isolated_sweep_root(label);
        super::create_private_dir_exclusive(&root).expect("create private isolated root");
        (root, guard)
    }

    /// `browser_transfer_root_bytes` must sum payload files across per-request
    /// subdirs (the input to the aggregate disk-cap guard). Uses tiny files so
    /// the test itself doesn't allocate near the cap.
    #[test]
    fn root_bytes_sums_payload_files_across_request_dirs() {
        let (root, _guard) = isolated_private_root("root_bytes");
        assert_eq!(
            super::browser_transfer_root_bytes(&root),
            0,
            "empty root is 0"
        );

        let d1 = root.join("req1");
        std::fs::create_dir_all(&d1).unwrap();
        std::fs::write(d1.join("a.bin"), vec![0u8; 100]).unwrap();
        let d2 = root.join("req2");
        std::fs::create_dir_all(&d2).unwrap();
        std::fs::write(d2.join("b.bin"), vec![0u8; 50]).unwrap();

        assert_eq!(super::browser_transfer_root_bytes(&root), 150);
    }

    struct IsolatedRootGuard(std::path::PathBuf);
    impl Drop for IsolatedRootGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Fresh request dirs younger than `max_age` must survive a sweep,
    /// otherwise the sweep would race in-flight uploads.
    #[test]
    fn sweep_preserves_fresh_request_dirs() {
        use super::sweep_browser_transfers_in;
        use std::time::Duration;

        let (root, _guard) = isolated_private_root("preserves_fresh");

        let fresh = root.join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&fresh).expect("create fresh request dir");
        std::fs::write(fresh.join("payload.bin"), b"fresh").expect("seed fresh payload");

        // A 60-minute threshold ensures the just-created dir is well
        // under the staleness cutoff regardless of CI clock skew.
        sweep_browser_transfers_in(&root, Duration::from_secs(3600));

        assert!(
            fresh.exists(),
            "sweep removed a fresh request dir it should have kept: {fresh:?}"
        );
    }

    /// Stale request dirs (older than `max_age`) must be removed —
    /// this is the actual job of the sweep. Deterministic because
    /// the threshold is `Duration::ZERO`, so everything counts as stale.
    #[test]
    fn sweep_removes_stale_request_dirs() {
        use super::sweep_browser_transfers_in;
        use std::time::Duration;

        let (root, _guard) = isolated_private_root("removes_stale");

        let stale = root.join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&stale).expect("create stale request dir");
        std::fs::write(stale.join("payload.bin"), b"stale").expect("seed stale payload");

        // `Duration::ZERO` makes every entry exceed the threshold
        // immediately, so the sweep MUST remove the dir.
        sweep_browser_transfers_in(&root, Duration::ZERO);

        assert!(
            !stale.exists(),
            "sweep failed to remove a stale request dir: {stale:?}"
        );
    }

    /// Missing root is the first-boot state. Sweep must not panic or
    /// create the directory itself, even when called against an
    /// absent path. Deterministic because the isolated root is
    /// guaranteed absent (uuid).
    #[test]
    fn sweep_is_a_noop_when_root_is_absent() {
        use super::sweep_browser_transfers_in;
        use std::time::Duration;

        let (root, _guard) = isolated_sweep_root("absent_root");
        // Deliberately do NOT create `root` — it's the missing-root
        // case we want to exercise.
        assert!(!root.exists(), "isolated root unexpectedly already exists");

        sweep_browser_transfers_in(&root, Duration::from_secs(3600));

        assert!(
            !root.exists(),
            "sweep should not create the root dir on its own: {root:?}"
        );
    }
}

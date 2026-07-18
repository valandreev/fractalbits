use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use data_types::TraceId;
use fractal_fuse::{FileHandleId, InodeId};
use rkyv::api::high::to_bytes_in;
use std::cell::Cell;
use std::os::fd::{AsRawFd, OwnedFd};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::backend::{BackendConfig, BlobInfo, StorageBackend};
use crate::cache::{DirCache, DirEntry, DirEntryKind};
use crate::config::WritebackMode;
use crate::disk_cache::DiskCache;
use crate::error::FsError;
use crate::inode::{EntryType, ForgetOutcome, InodeTable, ROOT_INODE};
use crate::writeback::{
    CoalesceOutcome, DrainableInodeIntent, Generation, InodeOp as WbInodeOp, WritebackQueue,
};
use data_types::object_layout::{
    DirectoryData, IndirectEntry, InodeRecord, MpuState, ObjectCoreMetaData, ObjectLayout,
    ObjectMetaData, ObjectState, PosixAttrs, SpecialData, SpecialKind, SymlinkData,
};
pub const TTL: Duration = Duration::from_secs(1);
pub const DEFAULT_BLOCK_SIZE: u32 = 128 * 1024;
/// Upper bound on a single file's in-memory write buffer. The buffer is
/// a flat `BytesMut`, so a truncate/extend allocates the whole size; a
/// target beyond this is rejected with EINVAL rather than attempting a
/// runaway allocation (which would abort the process).
pub const MAX_INMEM_FILE_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// Protocol-agnostic file/directory attributes.
#[derive(Debug, Clone, Copy)]
pub struct VfsAttr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime_secs: u64,
    pub mtime_secs: u64,
    pub ctime_secs: u64,
    /// Sub-second part of `atime`, in nanoseconds (0..1e9). Carried
    /// independently of `atime_secs` so a `utimensat` that set atime
    /// to (s, ns) round-trips through `lstat.atime_ns`.
    pub atime_ns_part: u32,
    pub mtime_ns_part: u32,
    pub ctime_ns_part: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
}

impl VfsAttr {
    /// Synthetic `VfsAttr` for a negative-dentry FUSE_LOOKUP reply.
    /// `ino == 0` is the FUSE protocol sentinel for "name does not
    /// exist"; combined with a non-zero entry TTL the kernel caches
    /// the absence and skips future LOOKUPs for the same name. The
    /// kernel reads only `nodeid` for negative entries, so the rest
    /// are zeros.
    pub fn negative_dentry() -> Self {
        Self {
            ino: 0,
            size: 0,
            blocks: 0,
            atime_secs: 0,
            mtime_secs: 0,
            ctime_secs: 0,
            atime_ns_part: 0,
            mtime_ns_part: 0,
            ctime_ns_part: 0,
            mode: 0,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VfsDirEntry {
    pub ino: u64,
    pub offset: u64,
    pub kind: DirEntryKind,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct VfsDirEntryPlus {
    pub ino: u64,
    pub offset: u64,
    pub kind: DirEntryKind,
    pub name: String,
    pub attr: VfsAttr,
}

#[derive(Debug, Clone, Copy)]
pub struct VfsStatfs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
}

thread_local! {
    static THREAD_BACKEND: Cell<Option<&'static StorageBackend>> = const { Cell::new(None) };
}

/// Per-block content intent for the sparse WriteBuffer.
///
/// Blocks NOT in the map are implicitly "Keep": no buffered work, BSS is
/// authoritative. The override flush uploads only `Rewrite` blocks (in
/// place at the bumped blob_version), replays `Delete` intents as
/// versioned block deletes, and never touches "Keep"/absent blocks. The
/// sparse buffer keeps in-memory ops O(1), avoids whole-file preload on
/// open, and serves dirty-handle reads per block.
#[derive(Debug, Clone)]
pub(crate) enum BlockState {
    /// Definitive new bytes for this block. Origin: `vfs_write`, a shrink
    /// tail-zero, or a punch-hole partial edge. The override flush uploads
    /// these (zero-padded to block_size) at the new blob_version.
    Rewrite(Bytes),
    /// PUNCH_HOLE intent: the override flush schedules a versioned
    /// `delete_block` so the BSS entry is dropped at the new blob_version.
    /// Reads (dirty-handle merge and post-flush via `BlockNotFound`) treat
    /// the block as zeros. Distinguished from a plain hole because a
    /// punched block sits inside the file's logical range and the deletion
    /// must be replayed on flush even with no `Rewrite` content.
    Delete,
}

pub(crate) struct WriteBuffer {
    /// Logical file size (includes holes). Authoritative within this
    /// handle session for stat / read clamping until flush commits.
    file_size: u64,
    /// True if `file_size` differs from the committed layout size at open
    /// time, or any block intent was buffered. Flush-eligibility predicate.
    size_changed: bool,
    /// Blob guid of the file at open time; used to lazy-load committed
    /// bytes for partial-block edits and dirty reads, and reused by the
    /// override flush. `None` for brand-new files.
    existing_blob_guid: Option<data_types::DataBlobGuid>,
    /// Block size copied from the committed layout (or DEFAULT for new
    /// files).
    block_size: u32,
    /// Per-block content intents, keyed by block index.
    blocks: std::collections::BTreeMap<u32, BlockState>,
    /// True if any flush-worthy work is buffered.
    dirty: bool,
    /// Smallest `ceil(new_size / block_size)` reached by any shrink in this
    /// session. Blocks at index `>= eof_low_watermark` had their committed
    /// BSS data logically destroyed by the shrink and must read as zeros
    /// until the flush trim deletes them, even if a later grow brings the
    /// index back into the file. Reset to `None` only on a successful
    /// flush. Without it, `truncate(small); write(past old EOF)` would
    /// lazy-load pre-shrink bytes and resurrect data POSIX requires zeroed.
    eof_low_watermark: Option<u32>,
    /// `committed_block_count` pinned at the FIRST shrink this session.
    /// Pairs with `eof_low_watermark` to bound the EOF-trim across
    /// post-CAS-failure retries: the flush promotes the committed size to
    /// the smaller new size, so recomputing the upper bound from the layout
    /// on retry would lose the original committed bound. Reset on flush.
    trim_upper: Option<u32>,
    /// Block indices fallocate has reserved. On flush these become
    /// `ReserveBlocks` (single-op, no batch) for blocks not superseded by a
    /// `Rewrite`/`Delete`. Reads and `lseek(SEEK_DATA)` treat reserved
    /// blocks as logical-data per Linux convention even before flush.
    pending_reservations: std::collections::BTreeSet<u32>,
}

impl WriteBuffer {
    fn new(
        existing_blob_guid: Option<data_types::DataBlobGuid>,
        file_size: u64,
        block_size: u32,
    ) -> Self {
        Self {
            file_size,
            size_changed: false,
            existing_blob_guid,
            block_size,
            blocks: std::collections::BTreeMap::new(),
            dirty: false,
            eof_low_watermark: None,
            trim_upper: None,
            pending_reservations: std::collections::BTreeSet::new(),
        }
    }

    /// Drop per-block intents and reservations past the new EOF (shrink).
    fn drop_blocks_past(&mut self, new_last_block_excl: u32) {
        self.blocks.retain(|b, _| *b < new_last_block_excl);
        self.pending_reservations
            .retain(|b| *b < new_last_block_excl);
    }

    /// True when block `b` sits in a range whose committed BSS bytes were
    /// destroyed by a shrink earlier this session; lazy-load and
    /// dirty-read paths must return zeros for such blocks.
    fn block_destroyed_by_shrink(&self, b: u32) -> bool {
        self.eof_low_watermark.is_some_and(|low| b >= low)
    }
}

struct FileHandle {
    ino: InodeId,
    s3_key: String,
    layout: Option<ObjectLayout>,
    write_buf: Option<WriteBuffer>,
    backing_id: Option<i32>,
}

/// A best-effort disk-cache mirror write, handed to the dedicated mirror
/// thread so the local-cache I/O + checksum never run on a FUSE worker.
struct MirrorJob {
    blob_guid: data_types::DataBlobGuid,
    blob_version: u64,
    rewrites: Vec<(u32, Bytes)>,
    deletes: Vec<u32>,
    /// Retained `rewrites` payload size, used to keep `mirror_queued_bytes`
    /// balanced (added on enqueue, subtracted once the job is processed).
    byte_len: usize,
}

/// Sender + the shared queued-byte counter for the mirror channel.
struct MirrorHandle {
    tx: futures::channel::mpsc::Sender<MirrorJob>,
    queued_bytes: Arc<AtomicUsize>,
}

/// Bound on queued mirror jobs by count. The dedicated thread drains local
/// page-cache writes far faster than the network publish feeds it, so this
/// rarely fills; when it does, `try_send` drops the job (best-effort; the
/// block cold-fills from BSS on the next read) instead of blocking a FUSE
/// worker.
const MIRROR_QUEUE_CAP: usize = 4096;

/// Byte bound on the mirror queue. A job retains its rewritten `Bytes`
/// until the mirror thread writes them, so a slow cache device could
/// otherwise pin unbounded flushed write buffers (one large-file override
/// flush is a single job but many MiB). When the in-flight payload exceeds
/// this, new jobs are dropped (best-effort) before their `Bytes` are
/// retained. 256 MiB caps memory while staying far above the steady-state
/// backlog of an 83k-file untar.
const MIRROR_BYTE_BUDGET: usize = 256 * 1024 * 1024;

/// Spawn the dedicated disk-cache mirror thread. It owns its own compio
/// runtime (separate io_uring) and drains the job channel, so a
/// create-heavy workload's cache writes never steal cycles from the FUSE
/// worker threads. Returns the handle, or `None` if the runtime could not
/// be built (mirror then silently disabled; the cache still serves
/// reads via cold-fill, just not write-populated).
fn spawn_mirror_worker(dc: Arc<DiskCache>) -> Option<MirrorHandle> {
    let (tx, mut rx) = futures::channel::mpsc::channel::<MirrorJob>(MIRROR_QUEUE_CAP);
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    let worker_bytes = queued_bytes.clone();
    let spawned = std::thread::Builder::new()
        .name("fb-disk-mirror".to_string())
        .spawn(move || {
            let rt = match compio_runtime::Runtime::builder().build() {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::warn!(error = %e, "disk-cache mirror thread: runtime build failed");
                    return;
                }
            };
            rt.block_on(async move {
                use futures::StreamExt;
                while let Some(job) = rx.next().await {
                    if let Err(e) = dc
                        .sync_after_flush(
                            job.blob_guid,
                            job.blob_version,
                            &job.rewrites,
                            &job.deletes,
                        )
                        .await
                    {
                        tracing::warn!(
                            blob_version = job.blob_version,
                            error = %e,
                            "disk cache mirror write failed (best-effort)"
                        );
                    }
                    worker_bytes.fetch_sub(job.byte_len, Ordering::Relaxed);
                }
            });
        });
    match spawned {
        Ok(_) => Some(MirrorHandle { tx, queued_bytes }),
        Err(e) => {
            tracing::warn!(error = %e, "failed to spawn disk-cache mirror thread");
            None
        }
    }
}

pub struct VfsCore {
    backend_config: Arc<BackendConfig>,
    inodes: Arc<InodeTable>,
    disk_cache: Option<Arc<DiskCache>>,
    dir_cache: DirCache,
    file_handles: DashMap<FileHandleId, FileHandle>,
    next_fh: AtomicU64,
    read_write: bool,
    passthrough_enabled: bool,
    passthrough_max_object_size: u64,
    prefetch_policy: crate::prefetch::PrefetchPolicy,
    /// Writeback queue. Always present, but only consulted when
    /// `writeback_mode` is `Default`. Worker is spawned lazily on
    /// the first FUSE op (the FUSE adapter's `init()` trait method
    /// is dead in this codebase; the session handles FUSE_INIT
    /// itself, so we spawn from inside the compio runtime when
    /// the first op arrives).
    writeback: Arc<WritebackQueue>,
    writeback_mode: WritebackMode,
    /// `max_batch_wait_ms` from the writeback config; the drainer
    /// polls this often.
    writeback_poll_ms: u32,
    /// One-shot guard for the writeback worker. Flipped by
    /// `ensure_writeback_worker` on first FUSE op.
    writeback_worker_started: AtomicBool,
    fuse_dev_fd: Option<Arc<OwnedFd>>,
    // Tracks blob data for unlinked files that still have open handles.
    // Cleanup is deferred until the last handle is released.
    deferred_blob_cleanup: DashMap<InodeId, Bytes>,
    // InodeId-scoped write lock. At most one write-mode handle per inode is
    // allowed. Map value is the owning fh so a stale lock for a closed fh
    // can be reclaimed by the next opener. Reads do not touch
    // this lock.
    inode_write_owner: DashMap<InodeId, FileHandleId>,
    // Handle to the dedicated disk-cache mirror thread. `None` when the
    // disk cache is disabled or the mirror thread failed to start. Keeps
    // the best-effort local-cache write off the FUSE worker threads so it
    // does not steal foreground cycles on a create-heavy workload.
    mirror: Option<MirrorHandle>,
}

mod attr;
mod dir;
mod drain;
mod namespace;
mod read;
mod write;

impl VfsCore {
    pub fn new(
        backend_config: Arc<BackendConfig>,
        inodes: Arc<InodeTable>,
        read_write: bool,
    ) -> Self {
        let config = &backend_config.config;
        let dir_cache_ttl = config.dir_cache_ttl();

        let disk_cache = if config.disk_cache_enabled {
            match DiskCache::new(
                &config.disk_cache_path,
                config.disk_cache_size_gb,
                DEFAULT_BLOCK_SIZE as u64,
            ) {
                Ok(dc) => {
                    tracing::info!(
                        path = %config.disk_cache_path,
                        size_gb = config.disk_cache_size_gb,
                        "disk cache enabled"
                    );
                    Some(Arc::new(dc))
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to init disk cache, falling back to no cache");
                    None
                }
            }
        } else {
            None
        };

        // The mirror thread owns a clone of the disk-cache handle and
        // drains queued writes off the FUSE worker threads.
        let mirror = disk_cache
            .as_ref()
            .and_then(|dc| spawn_mirror_worker(dc.clone()));

        let passthrough_enabled = config.passthrough_enabled;
        let passthrough_max_object_size =
            config.passthrough_max_object_size_gb * 1024 * 1024 * 1024;
        let prefetch_policy = crate::prefetch::PrefetchPolicy::from_config(config);
        // An unparseable mode is a misconfiguration: warn loudly and fall
        // back to Strict (fail-safe for durability) instead of silently
        // running a mode the operator did not ask for.
        let writeback_mode = WritebackMode::from_str(&config.writeback_mode).unwrap_or_else(|_| {
            tracing::warn!(
                value = %config.writeback_mode,
                "invalid FS_SERVER_WRITEBACK_MODE; falling back to strict"
            );
            WritebackMode::Strict
        });
        // Worker poll interval; honoured as configured (default 2ms). The
        // metadata path issues one put_inode per intent, so a large poll
        // just adds latency that drain_inode_to_barrier (every
        // unlink/rmdir/close) then waits out; keep the default tight. A
        // wake-on-enqueue notify would remove the residual poll latency
        // entirely and is the natural follow-up.
        let writeback_poll_ms = config.writeback_poll_ms.clamp(1, 1000);
        let writeback = Arc::new(WritebackQueue::new());

        Self {
            backend_config,
            inodes,
            disk_cache,
            dir_cache: DirCache::new(dir_cache_ttl),
            file_handles: DashMap::new(),
            next_fh: AtomicU64::new(1),
            read_write,
            passthrough_enabled,
            passthrough_max_object_size,
            prefetch_policy,
            writeback,
            writeback_mode,
            writeback_poll_ms,
            writeback_worker_started: AtomicBool::new(false),
            fuse_dev_fd: None,
            deferred_blob_cleanup: DashMap::new(),
            inode_write_owner: DashMap::new(),
            mirror,
        }
    }

    /// Install the shared `/dev/fuse` fd, obtained from
    /// `Session::fuse_fd()`, before the session is run. The fd is needed
    /// by passthrough open / close paths that may fire on the very first
    /// FUSE request.
    pub fn with_fuse_fd(mut self, fuse_dev_fd: Arc<OwnedFd>) -> Self {
        self.fuse_dev_fd = Some(fuse_dev_fd);
        self
    }

    // ── Internal helpers ──

    /// Get the per-thread StorageBackend, creating it on first access.
    /// The backend is leaked into 'static storage because each compio thread
    /// runs for the lifetime of the process and we need references that can
    /// be held across await points.
    fn backend(&self) -> &StorageBackend {
        THREAD_BACKEND.with(|cell| match cell.get() {
            Some(b) => b,
            None => {
                let b = Box::new(
                    StorageBackend::new(&self.backend_config)
                        .expect("Failed to create per-thread StorageBackend"),
                );
                let leaked: &'static StorageBackend = Box::leak(b);
                cell.set(Some(leaked));
                leaked
            }
        })
    }

    fn alloc_fh(&self) -> FileHandleId {
        FileHandleId(self.next_fh.fetch_add(1, Ordering::Relaxed))
    }

    fn dir_prefix(&self, ino: InodeId) -> Option<String> {
        self.inodes.get_s3_key(ino)
    }

    fn cache_dir_entry(&self, prefix: &str, name: &str, ino: InodeId, kind: DirEntryKind) {
        self.dir_cache.upsert(
            prefix,
            DirEntry {
                name: name.to_string(),
                ino: ino.0,
                kind,
            },
        );
    }

    fn dir_entry_kind_from_layout(layout: &ObjectLayout) -> DirEntryKind {
        match &layout.state {
            ObjectState::Symlink(_) => DirEntryKind::Symlink,
            ObjectState::Special(data) => match data.kind {
                SpecialKind::Fifo => DirEntryKind::NamedPipe,
                SpecialKind::BlockDevice => DirEntryKind::BlockDevice,
                SpecialKind::CharDevice => DirEntryKind::CharDevice,
                SpecialKind::Socket => DirEntryKind::Socket,
            },
            ObjectState::Directory(_) => DirEntryKind::Directory,
            _ => DirEntryKind::RegularFile,
        }
    }

    fn check_write_enabled(&self) -> Result<(), FsError> {
        if !self.read_write {
            return Err(FsError::ReadOnly);
        }
        Ok(())
    }

    fn has_open_handles_for_inode(&self, ino: InodeId, exclude_fh: Option<FileHandleId>) -> bool {
        self.file_handles.iter().any(|entry| {
            entry.value().ino == ino && exclude_fh.is_none_or(|excl| *entry.key() != excl)
        })
    }

    /// The inode's registered write-owner fh, if its buffer is dirty.
    /// Single-writer-per-inode makes this the only handle that can carry
    /// a dirty buffer (a reclaimed owner's handle is already gone from
    /// `file_handles`), so callers get O(1) instead of scanning every
    /// open handle on the hot open path.
    fn dirty_write_owner(&self, inode: InodeId) -> Option<FileHandleId> {
        let fh = self.inode_write_owner.get(&inode).map(|e| *e.value())?;
        self.file_handles
            .get(&fh)?
            .write_buf
            .as_ref()
            .is_some_and(|wb| wb.dirty)
            .then_some(fh)
    }

    /// Largest buffered file size across this inode's write handles: the
    /// in-memory EOF of a file whose first flush hasn't published yet.
    /// `0` when no write handle survives (e.g. the flush failed and the
    /// handle is gone). Single-writer-per-inode: the registered owner is
    /// the only handle that can hold a write buffer.
    /// Live size of the inode's dirty write buffer, or `None` when no
    /// write-mode handle currently holds one. Distinguishes "no dirty
    /// handle" from "dirty handle whose buffer is empty" (size 0), which
    /// the read-your-writes lookup path needs to decide whether the live
    /// buffer size should override a stale cached layout size.
    fn dirty_write_buffer_size(&self, ino: InodeId) -> Option<u64> {
        self.inode_write_owner
            .get(&ino)
            .map(|e| *e.value())
            .and_then(|fh| {
                self.file_handles
                    .get(&fh)
                    .and_then(|h| h.write_buf.as_ref().map(|wb| wb.file_size))
            })
    }

    fn dirty_buffer_size(&self, ino: InodeId) -> u64 {
        self.dirty_write_buffer_size(ino).unwrap_or(0)
    }

    /// Acquire the inode-scoped write lock for `fh`. Returns `Busy` if another
    /// write-mode handle currently owns it.
    ///
    /// Reclaim rule: if the recorded owner fh has been released (no entry in
    /// `file_handles`), the lock is stale and we take it. This recovers from
    /// any path that removes a handle without first calling
    /// `release_write_lock` (e.g. lookup races during shutdown).
    fn acquire_write_lock(&self, inode: InodeId, fh: FileHandleId) -> Result<(), FsError> {
        use dashmap::mapref::entry::Entry;
        match self.inode_write_owner.entry(inode) {
            Entry::Vacant(slot) => {
                slot.insert(fh);
                Ok(())
            }
            Entry::Occupied(mut slot) => {
                let owner = *slot.get();
                if !self.file_handles.contains_key(&owner) {
                    slot.insert(fh);
                    Ok(())
                } else {
                    Err(FsError::Busy)
                }
            }
        }
    }

    /// Acquire the inode write lock, briefly retrying to absorb the
    /// close-then-reopen-for-write race: a just-closed handle's FUSE_RELEASE
    /// (which drops this lock via `release_write_lock`) is asynchronous and
    /// may not have been processed by the time the kernel sends the next
    /// OPEN, so a single-process `write(); open(O_WRONLY)` would otherwise
    /// spuriously EBUSY (observed in truncate/O_TRUNC tests once per-flush
    /// latency grew). A genuinely concurrent writer keeps its handle open
    /// past the budget and still gets EBUSY.
    async fn acquire_write_lock_retry(
        &self,
        inode: InodeId,
        fh: FileHandleId,
    ) -> Result<(), FsError> {
        if self.acquire_write_lock(inode, fh).is_ok() {
            return Ok(());
        }
        // The lock may be held by an in-flight async close-flush:
        // FUSE_RELEASE spawns `vfs_release` off-thread and only drops the
        // write lock once the publish lands. Drain this inode's writeback
        // barrier so a re-open of a just-closed file (e.g. an O_TRUNC
        // reopen, or `echo x > f; cat f`) waits for the prior close to
        // commit (and reads its freshly published layout) instead of
        // spuriously failing EBUSY. No-op on an idle inode.
        self.drain_inode_to_barrier(inode).await?;
        if self.acquire_write_lock(inode, fh).is_ok() {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_millis(200);
        while Instant::now() < deadline {
            compio_runtime::time::sleep(Duration::from_millis(5)).await;
            // OPEN can beat the kernel's later RELEASE request for the
            // previous fd. Re-check the barrier in the retry loop so once
            // RELEASE registers its cycle, this path waits for the full
            // publish instead of timing out on the fixed dispatch window.
            self.drain_inode_to_barrier(inode).await?;
            if self.acquire_write_lock(inode, fh).is_ok() {
                return Ok(());
            }
        }
        Err(FsError::Busy)
    }

    fn release_write_lock(&self, inode: InodeId, fh: FileHandleId) {
        self.inode_write_owner
            .remove_if(&inode, |_, owner| *owner == fh);
    }

    // ── Attribute builders ──

    // ── Passthrough helpers ──

    /// Try to set up passthrough for a file handle. Returns (open_flags, backing_id)
    /// if passthrough is activated, or (0, 0) otherwise.
    pub fn try_passthrough(&self, fh: FileHandleId, layout: &ObjectLayout) -> (u32, i32) {
        if !self.passthrough_enabled {
            return (0, 0);
        }
        if self.read_write {
            // A read-write mount can later override this blob. Once the
            // kernel has a passthrough backing fd, metadata floors and cache
            // file unlinks cannot revoke that raw fd, so only arm passthrough
            // on read-only mounts.
            return (0, 0);
        }

        let dc = match &self.disk_cache {
            Some(dc) => dc,
            None => return (0, 0),
        };

        let file_size = match layout.size() {
            Ok(s) => s,
            Err(_) => return (0, 0),
        };

        // Skip large files
        if file_size > self.passthrough_max_object_size || file_size == 0 {
            return (0, 0);
        }

        let blob_guid = match layout.blob_guid() {
            Ok(g) => g,
            Err(_) => return (0, 0),
        };

        // Check if fully cached
        if !dc.is_complete(blob_guid, file_size) {
            return (0, 0);
        }

        let fuse_fd = match self.fuse_dev_fd.as_ref() {
            Some(fd) => fd.as_raw_fd(),
            None => return (0, 0),
        };

        // Open the cache file and register as backing fd
        let cache_path = dc.cache_file_path(blob_guid.blob_id, blob_guid.volume_id);
        let backing_file = match std::fs::File::open(&cache_path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "failed to open cache file for passthrough");
                return (0, 0);
            }
        };

        let backing_fd = backing_file.as_raw_fd();

        match fractal_fuse::passthrough::fuse_backing_open(fuse_fd, backing_fd) {
            Ok(bid) => {
                tracing::info!(fh = fh.0, backing_id = bid, "passthrough activated");
                // Store backing_id in file handle for cleanup
                if let Some(mut handle) = self.file_handles.get_mut(&fh) {
                    handle.backing_id = Some(bid);
                }
                (fractal_fuse::abi::FOPEN_PASSTHROUGH, bid)
            }
            Err(e) => {
                tracing::debug!(error = %e, "passthrough ioctl failed (not supported?)");
                (0, 0)
            }
        }
    }

    /// Try passthrough for an already-opened file handle.
    pub fn try_passthrough_for_fh(&self, fh: FileHandleId) -> Option<(u32, i32)> {
        let handle = self.file_handles.get(&fh)?;
        let layout = handle.layout.as_ref()?;
        Some(self.try_passthrough(fh, layout))
    }

    /// Clean up passthrough backing_id on file release.
    pub fn release_passthrough(&self, fh: FileHandleId) {
        let backing_id = self.file_handles.get(&fh).and_then(|h| h.backing_id);

        if let Some(bid) = backing_id
            && let Some(fuse_dev_fd) = self.fuse_dev_fd.as_ref()
            && let Err(e) =
                fractal_fuse::passthrough::fuse_backing_close(fuse_dev_fd.as_raw_fd(), bid)
        {
            tracing::warn!(backing_id = bid, error = %e, "failed to close backing");
        }
    }

    // ── Cache helpers ──

    // ── Read helpers ──

    // ── Zero-copy read helpers (direct-to-buffer) ──

    // ── Write helpers ──

    // ── Public VFS operations ──

    pub fn vfs_init(&self) {
        if let Some(dc) = &self.disk_cache {
            dc.spawn_evictor();
        }
        // Start the writeback worker here, on the FUSE lifecycle thread's
        // runtime. That runtime outlives the per-ring worker runtimes (it
        // drives `destroy` after every ring thread is joined), so the
        // worker keeps draining queued metadata through unmount instead of
        // dying with a ring runtime and leaving destroy to time out on a
        // dead drainer. `ensure_writeback_worker_started` is idempotent, so
        // the lazy calls on the metadata paths become no-ops.
        self.ensure_writeback_worker_started();
        tracing::info!("Filesystem initialized");
    }

    /// Spawn the writeback worker the first time it's needed. Cheap
    /// fast path: a relaxed atomic load + branch in steady state. The
    /// `compare_exchange` only fires once per process.
    fn ensure_writeback_worker_started(&self) {
        if self.writeback_mode != WritebackMode::Default {
            return;
        }
        if self.writeback_worker_started.load(Ordering::Relaxed) {
            return;
        }
        if self
            .writeback_worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        spawn_writeback_worker(
            Arc::clone(&self.backend_config),
            Arc::clone(&self.writeback),
            self.writeback_poll_ms,
        );
        tracing::info!(poll_ms = self.writeback_poll_ms, "writeback worker started");
    }

    pub fn vfs_destroy(&self) {
        // Block new enqueues; the worker keeps draining whatever is
        // already InFlight / Pending until the queue depth hits 0 or
        // the host process exits.
        if self.writeback_mode == WritebackMode::Default {
            self.writeback.set_enqueue_blocked(true);
            tracing::info!(
                queue_depth = self.writeback.depth(),
                "writeback enqueue blocked at destroy; draining residual"
            );
        }
        tracing::info!("Filesystem destroyed");
    }

    pub async fn vfs_open(&self, inode: InodeId, flags: u32) -> Result<FileHandleId, FsError> {
        let write_flags = libc::O_WRONLY as u32
            | libc::O_RDWR as u32
            | libc::O_APPEND as u32
            | libc::O_TRUNC as u32;
        let is_write = flags & write_flags != 0;

        if is_write && !self.read_write {
            return Err(FsError::ReadOnly);
        }

        {
            let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
            if entry.entry_type != EntryType::File {
                return Err(FsError::IsDir);
            }
        }

        // In default writeback mode, every open is the recovery point for a
        // deferred publish error. Read opens additionally publish any dirty
        // local handle inline first: the kernel sends RELEASE lazily after
        // close(2) returns (and a dup'ed fd can delay it), so waiting on
        // cycles alone could serve a stale pre-flush layout when OPEN wins
        // that race. Write opens do not flush another live writer; they just
        // drain any already-registered release cycle and let the write lock
        // below return EBUSY if the old writer is still open.
        if self.writeback_mode == WritebackMode::Default {
            if !is_write && let Some(dirty_fh) = self.dirty_write_owner(inode) {
                match self.flush_write_buffer(dirty_fh).await {
                    // The handle raced its release; the release path
                    // owns the flush now and the drain below waits it.
                    Err(FsError::BadFd) => {}
                    res => res?,
                }
            }
            self.drain_inode_to_barrier(inode).await?;
        }

        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
        let s3_key = entry.s3_key.clone();
        let layout = entry.layout.clone();
        let cached_inode_id = entry.inode_id;
        drop(entry);

        // Enforce single-writer per inode. The first writer
        // wins and subsequent write-mode opens fail with EBUSY. The lock
        // is process-local in-memory state and dies with the process on
        // crash, so the next open reacquires.
        let fh = self.alloc_fh();
        if is_write {
            self.acquire_write_lock_retry(inode, fh).await?;
        }

        // Resolve the layout (cold-fetch on a cache miss, then follow a
        // hardlink redirect to the shared record's real layout) and persist
        // any resolved hardlink identity back to the inode table. Wrapped so
        // a failure after the write lock was acquired still releases it;
        // otherwise the inode is left permanently EBUSY.
        //
        // Persisting the resolved `inode_id` is also what stops a cold-cache
        // Indirect entry (e.g. populated by readdirplus without a prior
        // vfs_lookup) from flushing a Normal layout over its redirect: the
        // flush keys its record-aware path on `entry.inode_id`. The redirect
        // itself has no blob_guid, so the resolved real layout is also what
        // lets the write buffer seed from the shared blob and reconcile at
        // the correct blob_version. Covers a cold cache (layout is
        // `Indirect`) and a warm one (cached `inode_id`, possibly a stale
        // pre-promotion layout copy).
        let resolved = async {
            let layout = match layout {
                Some(l) => Some(l),
                None => match self.backend().get_inode(&s3_key, &TraceId::new()).await {
                    Ok(l) => Some(l),
                    Err(FsError::NotFound) if is_write => None,
                    Err(FsError::NotFound) if !is_write => {
                        self.drain_inode_to_barrier(inode).await?;
                        match self.backend().get_inode(&s3_key, &TraceId::new()).await {
                            Ok(l) => Some(l),
                            Err(e) => return Err(e),
                        }
                    }
                    Err(e) => return Err(e),
                },
            };
            match layout {
                Some(l) => {
                    let (real, resolved_id) = if let Some(id) = cached_inode_id {
                        let real = self
                            .backend()
                            .get_inode_record(id, &TraceId::new())
                            .await?
                            .layout;
                        (real, Some(id))
                    } else if matches!(l.state, ObjectState::Indirect(_)) {
                        let (real, id, _nlink) = self.resolve_indirect(l, &TraceId::new()).await?;
                        (real, id)
                    } else {
                        (l, None)
                    };
                    if let Some(id) = resolved_id
                        && let Some(mut e) = self.inodes.get_mut(inode)
                    {
                        e.inode_id = Some(id);
                        e.layout = Some(real.clone());
                    }
                    Ok(Some(real))
                }
                None => Ok(None),
            }
        }
        .await;
        let layout = match resolved {
            Ok(l) => l,
            Err(e) => {
                if is_write {
                    self.release_write_lock(inode, fh);
                }
                return Err(e);
            }
        };

        // Cross-instance staleness reconciliation: if the cache file's
        // authoritative_blob_v lags the inode's blob_version, another
        // instance has bumped the version since we last sync'd. Clear
        // the cache file so subsequent reads cold-fetch from BSS.
        // Done on every open (read or write) so read-only handles
        // don't keep serving stale bytes.
        if let Some(dc) = &self.disk_cache
            && let Some(ref l) = layout
            && let Ok(blob_guid) = l.blob_guid()
            && let Err(e) = dc.reconcile_on_open(blob_guid, l.blob_version).await
        {
            tracing::warn!(
                %blob_guid, error = %e,
                "disk cache reconcile_on_open failed; continuing"
            );
        }

        let has_trunc = flags & libc::O_TRUNC as u32 != 0;
        let write_buf = if is_write {
            if let Some(ref l) = layout
                && !has_trunc
            {
                // Existing file, no O_TRUNC: seed a sparse buffer from the
                // committed geometry. No whole-file preload; partial-block
                // edits lazy-load only the blocks they touch.
                let blob_guid = l.blob_guid().ok();
                let committed_size = l.size().unwrap_or(0);
                Some(WriteBuffer::new(blob_guid, committed_size, l.block_size))
            } else if let Some(ref l) = layout {
                // O_TRUNC on an existing file: file_size 0, keep blob_guid so
                // the override flush trims the old blocks; size_changed/dirty
                // so flush sees the truncate. The committed layout size still
                // bounds the flush trim range.
                let blob_guid = l.blob_guid().ok();
                let mut wb = WriteBuffer::new(blob_guid, 0, l.block_size);
                wb.size_changed = true;
                wb.dirty = true;
                Some(wb)
            } else {
                // Brand-new file (NSS lookup returned NotFound).
                Some(WriteBuffer::new(None, 0, DEFAULT_BLOCK_SIZE))
            }
        } else {
            None
        };

        // Promote the cached entry to MRU on every open. Reads served
        // by `FUSE_PASSTHROUGH` bypass the per-block touch path
        // entirely, so without this hook a hot file served via
        // passthrough would never advance in LRU and the evictor would
        // treat it as cold.
        if !is_write
            && let Some(dc) = &self.disk_cache
            && let Some(ref l) = layout
            && let Ok(blob_guid) = l.blob_guid()
        {
            dc.touch_blob(blob_guid);
        }

        // Spawn a whole-blob prefetch when the open-time policy says
        // yes and the cache is not already complete. Read-only opens
        // only; writers own the blob's bytes via `WriteBuffer` and
        // have no need for a parallel prefetch.
        if !is_write
            && let Some(dc) = &self.disk_cache
            && let Some(ref l) = layout
            && let Ok(file_size) = l.size()
            && let Ok(blob_guid) = l.blob_guid()
        {
            let usage = dc.current_usage();
            let capacity = dc.capacity_bytes();
            // FOPEN_KEEP_CACHE is the kernel's sequential-read hint;
            // the open(2) flag itself does not directly map, so for
            // now we treat any non-O_RANDOM read as a candidate.
            // O_RANDOM is not a portable flag; absent it on Linux,
            // the conservative default is `false`; only the
            // full-threshold and workload_bulk_read branches fire.
            let keep_cache_hint = false;
            if !crate::prefetch::cache_pressure_high(usage, capacity, &self.prefetch_policy)
                && crate::prefetch::should_prefetch(
                    file_size,
                    keep_cache_hint,
                    &self.prefetch_policy,
                )
                && !dc.is_complete(blob_guid, file_size)
            {
                let dc_arc = Arc::clone(dc);
                let backend_cfg = Arc::clone(&self.backend_config);
                let layout_clone = l.clone();
                compio_runtime::spawn(async move {
                    spawn_prefetch_task(backend_cfg, dc_arc, layout_clone).await;
                })
                .detach();
            }
        }

        self.file_handles.insert(
            fh,
            FileHandle {
                ino: inode,
                s3_key,
                layout,
                write_buf,
                backing_id: None,
            },
        );

        Ok(fh)
    }
}

/// Background whole-blob prefetch. Walks every block of `layout`,
/// fetches it from BSS, and inserts it into the disk cache. Each
/// per-block fetch goes through the same path as a read miss
/// (`backend.read_block` + `dc.insert`) so block_id, version, and
/// checksum semantics stay identical between prefetch-warmed entries
/// and lazy-warmed ones.
///
/// Errors are logged and ignored: a prefetch is best-effort, and a
/// transient failure is acceptable; the kernel's block-on-demand
/// path still serves the read.
async fn spawn_prefetch_task(
    backend_cfg: Arc<BackendConfig>,
    disk_cache: Arc<DiskCache>,
    layout: ObjectLayout,
) {
    let Ok(file_size) = layout.size() else {
        return;
    };
    if file_size == 0 {
        return;
    }
    let Ok(blob_guid) = layout.blob_guid() else {
        return;
    };
    let block_size = layout.block_size as u64;
    if block_size == 0 {
        return;
    }
    // Re-check pressure: an unrelated workload may have filled the
    // cache between the open-time decision and the task starting.
    let policy = crate::prefetch::PrefetchPolicy {
        full_threshold_bytes: u64::MAX,
        partial_threshold_bytes: u64::MAX,
        workload_bulk_read: false,
        // Reuse the cache's high-watermark fraction for the in-task
        // pressure decline.
        pressure_decline: 0.95,
    };
    if crate::prefetch::cache_pressure_high(
        disk_cache.current_usage(),
        disk_cache.capacity_bytes(),
        &policy,
    ) {
        return;
    }

    let backend = match StorageBackend::new(&backend_cfg) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "prefetch: failed to construct backend");
            return;
        }
    };

    let last_block = ((file_size - 1) / block_size) as u32;
    let trace_id = TraceId::new();

    for block_num in 0..=last_block {
        let block_start = block_num as u64 * block_size;
        let block_content_len = std::cmp::min(block_size, file_size - block_start) as usize;

        // If another path has already populated this block (e.g. a
        // racing read), the cache hit short-circuits the BSS round
        // trip.
        if disk_cache
            .get_block(blob_guid, block_num, block_content_len)
            .await
            .is_some()
        {
            continue;
        }

        // Override (blob_version > 1) blocks are padded to block_size on
        // disk; request the full block so the EC shard size matches, then
        // truncate to the logical content length (mirrors read_block_cached).
        let read_len = if layout.blob_version > 1 {
            (DEFAULT_BLOCK_SIZE as usize).max(block_content_len)
        } else {
            block_content_len
        };
        let (mut data, _checksum) = match backend
            .read_block(
                blob_guid,
                layout.blob_version,
                block_num,
                read_len,
                &trace_id,
            )
            .await
        {
            Ok(r) => r,
            Err(FsError::Rpc(rpc_client_common::RpcError::NotFound)) => {
                // Sparse hole; intentionally not cached. The
                // block-on-demand path treats missing blocks as zeros.
                continue;
            }
            Err(e) => {
                tracing::debug!(
                    %blob_guid, block_num, error = %e,
                    "prefetch block fetch failed; abandoning prefetch"
                );
                return;
            }
        };
        if data.len() > block_content_len {
            data = data.slice(0..block_content_len);
        }

        let _ = disk_cache
            .insert_block(blob_guid, block_num, layout.blob_version, &data)
            .await;
    }
}

/// Long-running writeback worker. Polls the queue every `poll_ms`,
/// drains pending intents, and fires NSS `put_inode` for each.
/// Spawned at FUSE init when `WritebackMode::Default` is configured;
/// runs until the process exits. Each intent ships as a single-op
/// `put_inode` RPC; the pipelining win comes from overlapping many such
/// round-trips concurrently, not from coalescing them.
///
/// Max concurrent `put_inode` RPCs per drained batch. Intents in a batch
/// are on distinct inodes (see `drain_pending`), so they publish in
/// parallel; the cap bounds in-flight RPCs against NSS.
const PUBLISH_CONCURRENCY: usize = 32;

fn spawn_writeback_worker(
    backend_cfg: Arc<BackendConfig>,
    queue: Arc<WritebackQueue>,
    poll_ms: u32,
) {
    let poll_dur = Duration::from_millis(poll_ms.max(1) as u64);
    compio_runtime::spawn(async move {
        // One backend per concurrent publish lane. StorageBackend has
        // RefCell-backed clients so independent futures must not share one
        // instance across awaits, especially when failover refresh mutates the
        // cached NSS client.
        let mut backends = Vec::with_capacity(PUBLISH_CONCURRENCY);
        for lane in 0..PUBLISH_CONCURRENCY {
            match StorageBackend::new(&backend_cfg) {
                Ok(b) => backends.push(b),
                Err(e) => {
                    tracing::warn!(
                        lane,
                        error = %e,
                        "writeback worker: failed to init backend; aborting"
                    );
                    return;
                }
            }
        }

        loop {
            compio_runtime::time::sleep(poll_dur).await;

            // Drain a batch of pending intents. The drainer flips them
            // to InFlight before returning so concurrent enqueues fall
            // into the next-cycle / backpressure path.
            let drained = queue.drain_pending(1024);
            if drained.is_empty() {
                continue;
            }

            // Publish independent intents concurrently. `drain_pending`
            // returns at most one generation per inode, so no two intents in
            // the batch touch the same inode; they are order-independent and
            // safe to fire together. Bounded chunks cap the fan-out on NSS so
            // a large batch cannot open thousands of in-flight RPCs at once.
            let queue = &queue;
            for chunk in drained.chunks(PUBLISH_CONCURRENCY) {
                futures::future::join_all(chunk.iter().enumerate().map(|(lane, intent)| {
                    let backend = &backends[lane];
                    async move {
                        let inode = intent.inode;
                        match publish_intent_with_retry(backend, intent).await {
                            Ok(_) => {
                                queue.mark_committed(&intent.s3_key, intent.generation, inode);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    key = %intent.s3_key,
                                    generation = intent.generation.0,
                                    error = %e,
                                    "writeback publish failed"
                                );
                                queue.mark_failed(&intent.s3_key, intent.generation, inode);
                            }
                        }
                    }
                }))
                .await;
            }
        }
    })
    .detach();
}

/// Releases a `block_prefix` hold when a directory rename returns or
/// unwinds, so a blocked prefix never outlives the rename that set it
/// (e.g. an early `?` on a drain or `rename_folder` error, or future
/// cancellation at unmount).
struct PrefixBlockGuard {
    writeback: Arc<WritebackQueue>,
    prefix: String,
}

impl Drop for PrefixBlockGuard {
    fn drop(&mut self) {
        self.writeback.unblock_prefix(&self.prefix);
    }
}

/// Collapses a release-flush cycle to `Done` when the flush task is dropped
/// before it can advance the cycle itself (the ring runtime hosting the
/// detached task is torn down at unmount). An orphaned non-`Done` cycle
/// would otherwise wedge `destroy`'s drain barrier until it times out. The
/// paired `FlushSnapshotGuard` has by then restored the buffer dirty, so
/// `destroy`'s `flush_open_dirty_handles` still republishes the data.
/// Disarmed on the normal paths, which advance the cycle explicitly.
struct ReleaseCycleGuard {
    writeback: Arc<WritebackQueue>,
    ino: InodeId,
    generation: Generation,
    armed: bool,
}

impl Drop for ReleaseCycleGuard {
    fn drop(&mut self) {
        if self.armed {
            self.writeback.advance_to_done(self.ino, self.generation);
        }
    }
}

/// Restores a flush's taken block snapshot back into the file handle if the
/// flush does not complete: on an error return OR on future cancellation
/// (e.g. a release-flush task dropped when its ring runtime is torn down at
/// unmount). `flush_write_buffer` moves the blocks out and clears `dirty`
/// up front; without this guard a cancelled flush would leave the handle
/// looking clean, so `destroy`'s `flush_open_dirty_handles` would skip it
/// and the buffered data would be silently lost. Disarmed once the publish
/// succeeds, after which the snapshot is discarded normally.
struct FlushSnapshotGuard<'a> {
    vfs: &'a VfsCore,
    fh_id: FileHandleId,
    blocks: std::collections::BTreeMap<u32, BlockState>,
    pending_reservations: std::collections::BTreeSet<u32>,
    armed: bool,
}

impl Drop for FlushSnapshotGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.vfs.restore_flush_snapshot(
                self.fh_id,
                std::mem::take(&mut self.blocks),
                std::mem::take(&mut self.pending_reservations),
            );
        }
    }
}

/// Absence-guarded create that tolerates an internally-retried RPC whose
/// first attempt committed but whose reply was lost. A blind `put_inode`
/// was idempotent under such a retry; the CAS-on-absence is not: the
/// re-sent attempt sees the key present and returns `CasConflict` against
/// the mount's own committed layout. On `CasConflict`, re-fetch and
/// compare bytes: if the stored inode byte-equals what we are publishing it
/// is our own commit (success); otherwise a peer won the name (a real
/// `CasConflict`). A peer's create never matches because the layout carries
/// a per-publish `version_id`.
async fn put_inode_create_idempotent(
    backend: &StorageBackend,
    key: &str,
    layout_bytes: Bytes,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    match backend
        .put_inode_cas(key, layout_bytes.clone(), Bytes::new(), trace_id)
        .await
    {
        Ok(_) => Ok(()),
        Err(FsError::CasConflict) => match backend.get_inode(key, trace_id).await {
            Ok(cur) => {
                let cur_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&cur, Vec::new())
                    .map(Bytes::from)
                    .map_err(FsError::from)?;
                if cur_bytes == layout_bytes {
                    Ok(())
                } else {
                    Err(FsError::CasConflict)
                }
            }
            // The key vanished between the CAS and this fetch (a concurrent
            // delete): treat as a lost race, not our own commit.
            Err(FsError::NotFound) => Err(FsError::CasConflict),
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    }
}

/// Ship one intent to NSS with bounded retries, so a transient backend
/// blip doesn't taint the inode and silently drop metadata the caller
/// already saw succeed.
async fn publish_intent_with_retry(
    backend: &StorageBackend,
    intent: &DrainableInodeIntent,
) -> Result<(), FsError> {
    const MAX_ATTEMPTS: u32 = 3;
    let mut result = Ok(());
    for attempt in 1..=MAX_ATTEMPTS {
        let trace_id = TraceId::new();
        result = match &intent.op {
            // Brand-new entry create. Guard on absence (empty expected
            // bytes) so a peer that created the same name during the
            // async window is not blindly overwritten; a lost race
            // surfaces as CasConflict, taints the inode, and the caller
            // re-looks-up the winner.
            WbInodeOp::PutInode { layout_bytes, .. } => {
                put_inode_create_idempotent(
                    backend,
                    &intent.s3_key,
                    layout_bytes.clone(),
                    &trace_id,
                )
                .await
            }
            WbInodeOp::SetPosix {
                posix,
                expected_layout_bytes,
                layout_bytes,
            } => {
                publish_set_posix(
                    backend,
                    &intent.s3_key,
                    posix,
                    expected_layout_bytes,
                    layout_bytes,
                    &trace_id,
                )
                .await
            }
        };
        match &result {
            Ok(()) => return Ok(()),
            // An absence-guarded create that hits CasConflict lost the
            // name to a peer; that is terminal (retrying can only lose
            // again), so surface it now to taint and re-lookup. SetPosix
            // keeps the outer retry: its own fold loop re-fetches fresh
            // state, so a later attempt can still win a bursty conflict.
            Err(FsError::CasConflict) if matches!(intent.op, WbInodeOp::PutInode { .. }) => {
                return result;
            }
            Err(e) if attempt < MAX_ATTEMPTS => {
                tracing::warn!(
                    key = %intent.s3_key,
                    attempt,
                    error = %e,
                    "writeback publish retrying"
                );
                compio_runtime::time::sleep(Duration::from_millis(20 * attempt as u64)).await;
            }
            Err(_) => {}
        }
    }
    result
}

/// Apply a posix-only update via CAS. Fast path: one `put_inode_cas`
/// guarded on the layout snapshot taken at enqueue. On conflict the
/// fresh layout is fetched and the posix folded onto it, so a
/// concurrent data publish (close-flush CAS) is never rolled back to
/// the enqueue-time blob state. A missing key means the entry was
/// deleted after the enqueue; the update is moot.
async fn publish_set_posix(
    backend: &StorageBackend,
    key: &str,
    posix: &PosixAttrs,
    expected: &Bytes,
    folded: &Bytes,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    match backend
        .put_inode_cas(key, folded.clone(), expected.clone(), trace_id)
        .await
    {
        Ok(_) => return Ok(()),
        Err(FsError::CasConflict) => {}
        Err(FsError::NotFound) => return Ok(()),
        Err(e) => return Err(e),
    }
    const MAX_CAS_RETRIES: u32 = 4;
    for _ in 0..MAX_CAS_RETRIES {
        let cur = match backend.get_inode(key, trace_id).await {
            Ok(l) => l,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        };
        // A concurrent hardlink promotion moved the posix into the
        // shared record; follow the redirect and publish there instead
        // of folding metadata into the redirect row.
        if let ObjectState::Indirect(redirect) = &cur.state {
            return publish_set_posix_record(backend, redirect.inode_id, posix, trace_id).await;
        }
        let cur_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&cur, Vec::new())
            .map_err(FsError::from)?
            .into();
        let new_layout = crate::inode::layout_with_posix(cur, *posix);
        let new_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&new_layout, Vec::new())
            .map_err(FsError::from)?
            .into();
        match backend
            .put_inode_cas(key, new_bytes, cur_bytes, trace_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(FsError::CasConflict) => continue,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
    Err(FsError::CasConflict)
}

async fn publish_set_posix_record(
    backend: &StorageBackend,
    inode_id: uuid::Uuid,
    posix: &PosixAttrs,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    const MAX_CAS_RETRIES: u32 = 4;
    let key = InodeRecord::key_for(inode_id);
    for _ in 0..MAX_CAS_RETRIES {
        let mut record = match backend.get_inode_record(inode_id, trace_id).await {
            Ok(record) => record,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        };
        let old_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
            .map_err(FsError::from)?
            .into();
        record.layout = crate::inode::layout_with_posix(record.layout.clone(), *posix);
        let new_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
            .map_err(FsError::from)?
            .into();
        match backend
            .put_inode_cas(&key, new_bytes, old_bytes, trace_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(FsError::CasConflict) => continue,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
    Err(FsError::CasConflict)
}

/// Extract the parent prefix from an s3_key.
/// e.g. "/foo/bar" -> "/foo/", "/top" -> "/"
fn parent_prefix_of(key: &str) -> String {
    let trimmed = key.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(pos) => trimmed[..=pos].to_string(),
        None => "/".to_string(),
    }
}

/// Wall-clock nanoseconds since the Unix epoch. `0` on the (impossible)
/// pre-epoch clock so callers can treat `0` as the uninitialised
/// sentinel.
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn file_mode(perm: u16) -> u32 {
    libc::S_IFREG | perm as u32
}

fn dir_mode(perm: u16) -> u32 {
    libc::S_IFDIR | perm as u32
}

fn symlink_mode(perm: u16) -> u32 {
    libc::S_IFLNK | perm as u32
}

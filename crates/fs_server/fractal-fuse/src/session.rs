use std::collections::HashSet;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;

use compio_runtime::Runtime;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const IORING_REGISTER_FILES: libc::c_uint = 2;
const IORING_UNREGISTER_FILES: libc::c_uint = 3;

/// Register fixed files with the current thread's io_uring instance.
///
/// This calls the `io_uring_register` syscall directly because the published
/// compio-runtime (0.11.x) does not yet expose `register_files`.
fn register_files(fds: &[i32]) -> io::Result<()> {
    let ring_fd = Runtime::with_current(|rt| rt.as_raw_fd());
    let ret = unsafe {
        libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd as libc::c_uint,
            IORING_REGISTER_FILES,
            fds.as_ptr(),
            fds.len() as libc::c_uint,
        )
    };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Unregister fixed files from the current thread's io_uring instance.
fn unregister_files() -> io::Result<()> {
    let ring_fd = Runtime::with_current(|rt| rt.as_raw_fd());
    let ret = unsafe {
        libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd as libc::c_uint,
            IORING_UNREGISTER_FILES,
            std::ptr::null::<libc::c_void>(),
            0u32,
        )
    };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

use crate::abi::*;
use crate::dispatch;
use crate::filesystem::{Filesystem, FsResult};
use crate::mount::{self, MountOptions};
use crate::ring::*;
use crate::types::{ReplyInit, Request};

/// Default max_write size (1MB).
const DEFAULT_MAX_WRITE: u32 = 1024 * 1024;

/// FUSE session managing the lifecycle from mount to shutdown.
pub struct Session {
    mount_path: PathBuf,
    mount_options: MountOptions,
    fd: Arc<OwnedFd>,
    queue_depth: u16,
    queue_count: usize,
    max_write: u32,
    shutdown: CancellationToken,
}

impl Session {
    pub fn new(mount_path: PathBuf, mount_options: MountOptions) -> io::Result<Self> {
        info!("mounting FUSE filesystem at {:?}", mount_path);
        let fd = mount::fusermount(&mount_options, &mount_path)?;
        info!("FUSE fd: {}", fd.as_raw_fd());
        Ok(Self {
            mount_path,
            mount_options,
            fd: Arc::new(fd),
            queue_depth: DEFAULT_QUEUE_DEPTH,
            queue_count: num_cpus(),
            max_write: DEFAULT_MAX_WRITE,
            shutdown: CancellationToken::new(),
        })
    }

    /// Token to signal [`Session::run`](crate::Session::run) to terminate after in-flight
    /// requests are completed
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Number of io_uring entries per queue (defaults to `DEFAULT_QUEUE_DEPTH` = 256).
    pub fn with_queue_depth(mut self, depth: u16) -> Self {
        self.queue_depth = depth;
        self
    }

    /// Number of threads to spawn driving io_uring queues (defaults to num CPUs).
    pub fn with_queue_count(mut self, threads: usize) -> Self {
        self.queue_count = threads;
        self
    }

    /// Maximum FUSE write payload (bytes), advertised to the kernel in FUSE_INIT
    /// (defaults to `DEFAULT_MAX_WRITE` = 1 MiB).
    pub fn with_max_write(mut self, max_write: u32) -> Self {
        self.max_write = max_write;
        self
    }

    /// Negotiate FUSE_INIT, setup io_uring rings, and run until shutdown.
    /// This function blocks the calling thread.
    pub fn run<F: Filesystem>(self, fs: F) -> io::Result<()> {
        let result = self.run_inner(fs);

        if let Ok(true) | Err(_) = result {
            // Phase 4: Unmount
            info!("unmounting {:?}", self.mount_path);
            if let Err(e) = mount::fusermount_unmount(&self.mount_path) {
                warn!("unmount failed: {}", e);
            }
        }

        result.map(|_| ())
    }

    /// Returns whether we think the filesystem is still mounted
    fn run_inner<F: Filesystem>(&self, fs: F) -> io::Result<bool> {
        let fs = Arc::new(fs);
        // Phase 2: read kernel FUSE_INIT, run fs.init() on the lifecycle
        // thread, then write the kernel reply.
        let parsed = read_fuse_init(self.fd.as_fd())?;
        let init_request = parsed.request;

        // The lifecycle thread hosts a compio runtime dedicated to the
        // filesystem's lifecycle hooks (init, destroy). It stays alive for
        // the whole mount so any background tasks those hooks spawn (e.g.
        // the disk-cache evictor's 60s timer) keep being polled. /dev/fuse
        // never delivers FUSE_DESTROY (see abi.rs FUSE_DESTROY note), so
        // destroy is signaled by the LifecycleGuard drop below.
        let destroy_signal = CancellationToken::new();
        let (init_tx, init_rx) = mpsc::sync_channel::<FsResult<ReplyInit>>(1);
        let lifecycle_fs = fs.clone();
        let lifecycle_destroy = destroy_signal.clone();
        let fuse_dev_fd = self.fd.clone();
        let lifecycle_thread = thread::Builder::new()
            .name("fuse-lifecycle".to_string())
            .spawn(move || -> io::Result<()> {
                let rt = Runtime::builder().build().map_err(|e| {
                    error!("failed to create lifecycle runtime: {e}");
                    e
                })?;
                rt.block_on(async {
                    match lifecycle_fs.init(init_request, fuse_dev_fd).await {
                        Ok(reply) => {
                            // Send reply before awaiting destroy so the
                            // main thread can resume mount setup while
                            // background tasks keep running here.
                            let _ = init_tx.send(Ok(reply));
                            lifecycle_destroy.cancelled().await;
                            lifecycle_fs.destroy().await;
                        }
                        Err(errno) => {
                            // init failed: no destroy. Thread exits, the
                            // runtime drops, any spawned tasks are cancelled.
                            let _ = init_tx.send(Err(errno));
                        }
                    }
                });
                Ok(())
            })?;

        let reply = match init_rx.recv() {
            Ok(Ok(r)) => r,
            Ok(Err(errno)) => {
                let _ = lifecycle_thread.join();
                return Err(io::Error::other(format!(
                    "fs.init() failed: errno {}",
                    errno
                )));
            }
            Err(_) => {
                let join_result = lifecycle_thread.join();
                return Err(io::Error::other(format!(
                    "lifecycle thread exited before init completed: {:?}",
                    join_result
                )));
            }
        };

        // From here on, fs.destroy() must run on every exit path. The
        // guard's Drop cancels the destroy signal and joins the lifecycle
        // thread, which runs destroy on its own runtime.
        let _lifecycle = LifecycleGuard {
            token: destroy_signal,
            thread: Some(lifecycle_thread),
        };

        // Cap the FS-requested max_write at the session-configured ceiling
        // so ring buffer allocation (max_payload below) cannot underflow
        // what we advertise to the kernel.
        let max_write = self.max_write.min(reply.max_write);
        write_fuse_init_reply(
            self.fd.as_fd(),
            &parsed,
            max_write,
            &reply,
            &self.mount_options,
        )?;

        let max_payload = max_write as usize;
        let queue_depth = self.queue_depth;

        info!(
            "FUSE_INIT done: max_write={}, queues={}, depth={}",
            max_write, self.queue_count, queue_depth
        );

        // Phase 3: Spawn per-CPU ring threads, each with its own compio Runtime
        let mut threads = Vec::with_capacity(self.queue_count);
        let connected = Arc::new(AtomicBool::new(true));
        let fuse_raw_fd = self.fd.as_raw_fd();
        for queue_id in 0..self.queue_count {
            let fs = fs.clone();
            let shutdown = self.shutdown.clone();
            let connected = connected.clone();

            let spawn_result = thread::Builder::new()
                .name(format!("fuse-q{}", queue_id))
                .spawn(move || {
                    let mut cpus = HashSet::new();
                    cpus.insert(queue_id);

                    let rt = Runtime::builder()
                        .thread_affinity(cpus)
                        .build()
                        .expect("cannot create compio runtime");

                    rt.block_on(async {
                        match run_queue(
                            fuse_raw_fd,
                            queue_id as u16,
                            queue_depth,
                            max_payload,
                            fs,
                            shutdown,
                        )
                        .await
                        {
                            Ok(queue_connected) => {
                                connected.fetch_and(queue_connected, Ordering::Relaxed);
                            }
                            Err(e) => error!("queue {} failed: {}", queue_id, e),
                        }
                    });
                });

            match spawn_result {
                Ok(handle) => threads.push(handle),
                Err(e) => {
                    // Partial-spawn cleanup: already-started queues hold
                    // clones of `fs` and the raw FUSE fd. Cancel the
                    // shutdown token to wind them down, then join them
                    // before returning. This must complete before the
                    // LifecycleGuard's drop runs fs.destroy(), or destroy
                    // would race with live queue threads.
                    error!(
                        "failed to spawn queue {} (after starting {}): {}",
                        queue_id,
                        threads.len(),
                        e
                    );
                    self.shutdown.cancel();
                    for h in threads {
                        h.join().unwrap_or_else(|p| {
                            error!("ring thread panicked during cleanup: {:?}", p);
                        });
                    }
                    return Err(e);
                }
            }
        }

        // Wait for all ring threads to complete
        for handle in threads {
            handle.join().unwrap_or_else(|e| {
                error!("ring thread panicked: {:?}", e);
            });
        }

        // _lifecycle drops here, signaling destroy and joining the thread.
        Ok(connected.load(Ordering::Relaxed))
    }
}

/// RAII guard that drives `Filesystem::destroy` and joins the lifecycle
/// thread when dropped. Constructed only after `init` succeeded, so the
/// destroy hook runs on every post-init exit path (including panics and
/// early returns from later setup steps).
struct LifecycleGuard {
    token: CancellationToken,
    thread: Option<thread::JoinHandle<io::Result<()>>>,
}

impl Drop for LifecycleGuard {
    fn drop(&mut self) {
        self.token.cancel();
        if let Some(t) = self.thread.take() {
            match t.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("lifecycle thread error: {}", e),
                Err(e) => error!("lifecycle thread panicked: {:?}", e),
            }
        }
    }
}

/// Run a single queue on a compio runtime thread.
/// Spawns an independent task per ring entry so they can process requests
/// concurrently (register blocks until the kernel sends a request, so
/// sequential registration would deadlock).
async fn run_queue<F: Filesystem>(
    fuse_raw_fd: i32,
    queue_id: u16,
    queue_depth: u16,
    max_payload: usize,
    fs: Arc<F>,
    shutdown: CancellationToken,
) -> io::Result<bool> {
    // Register fuse fd with this thread's io_uring
    register_files(&[fuse_raw_fd])?;

    debug!(
        "queue {}: registered fuse fd, allocating {} entries",
        queue_id, queue_depth
    );

    // Allocate page-aligned buffers
    let entries = allocate_ring_entries(queue_depth, max_payload)?;

    // Spawn independent task per entry: each registers with the kernel and
    // then loops dispatching requests. This avoids deadlock since REGISTER
    // blocks until the kernel delivers a request to that entry.
    let mut handles = Vec::with_capacity(entries.len());
    for mut entry in entries {
        let fs = fs.clone();
        let shutdown = shutdown.clone();
        let handle =
            compio_runtime::spawn(
                async move { run_entry(queue_id, &mut entry, &*fs, &shutdown).await },
            );
        handles.push(handle);
    }

    let mut connected = true;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.kind() == io::ErrorKind::NotConnected => {
                connected = false;
            }
            Ok(Err(e)) => error!("entry task failed: {}", e),
            Err(e) => error!("entry task panicked: {:?}", e),
        }
    }

    unregister_files()?;
    Ok(connected)
}

/// Run a single ring entry: register with the kernel, then loop
/// dispatching requests and committing responses.
async fn run_entry<F: Filesystem>(
    queue_id: u16,
    entry: &mut RingEntry,
    fs: &F,
    shutdown: &CancellationToken,
) -> io::Result<()> {
    // Register this entry's buffers with the kernel (blocks until first request)
    if submit_cancelable(shutdown, "register", FuseRegister::new(entry, queue_id)).await? {
        return Ok(());
    }

    // Process requests in a loop: dispatch -> commit response + fetch next
    loop {
        let needs_response = dispatch::dispatch(fs, entry).await;

        if needs_response.is_none() {
            // FORGET-type op: re-register without sending a response
            if submit_cancelable(shutdown, "re-register", FuseRegister::new(entry, queue_id))
                .await?
            {
                break;
            }
            continue;
        }

        // Commit response + fetch next request
        let commit_id = entry.commit_id();
        if submit_cancelable(
            shutdown,
            "commit",
            FuseCommitAndFetch::new(queue_id, commit_id),
        )
        .await?
        {
            break;
        }
    }

    Ok(())
}

/// Returns whether to shut down
async fn submit_cancelable<T: compio_driver::OpCode + 'static>(
    token: &CancellationToken,
    op_name: &'static str,
    op: T,
) -> io::Result<bool> {
    let result = token.run_until_cancelled(compio_runtime::submit(op)).await;
    match result.map(|x| x.0) {
        Some(Ok(_)) => Ok(false),
        Some(Err(e)) if e.kind() == io::ErrorKind::NotConnected => Err(e),
        Some(Err(e)) => {
            error!("FUSE {op_name} failed: {e}");
            Err(io::Error::other(e.to_string()))
        }
        // Shutting down
        None => Ok(true),
    }
}

/// Parsed FUSE_INIT request from the kernel, retained until the reply is
/// sent so we can echo back `unique` and intersect capability flags.
struct ParsedFuseInit {
    unique: u64,
    request: Request,
    kernel_flags: u64,
    kernel_max_readahead: u32,
}

/// Read and parse the kernel's FUSE_INIT request over blocking /dev/fuse.
fn read_fuse_init(fuse_fd: BorrowedFd<'_>) -> io::Result<ParsedFuseInit> {
    let mut buf = vec![0u8; 8192];
    let n = nix::unistd::read(fuse_fd, &mut buf).map_err(io::Error::from)?;
    if n < std::mem::size_of::<fuse_in_header>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FUSE_INIT read too short",
        ));
    }

    let in_hdr = unsafe { &*(buf.as_ptr() as *const fuse_in_header) };
    if in_hdr.opcode != FUSE_INIT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected FUSE_INIT, got opcode {}", in_hdr.opcode),
        ));
    }

    let unique = in_hdr.unique;
    let request = Request {
        unique,
        uid: in_hdr.uid,
        gid: in_hdr.gid,
        pid: in_hdr.pid,
    };

    let in_body_offset = std::mem::size_of::<fuse_in_header>();
    let init_in = unsafe { &*(buf.as_ptr().add(in_body_offset) as *const fuse_init_in) };

    let major = init_in.major;
    let minor = init_in.minor;
    info!(
        "FUSE_INIT: kernel version {}.{}, max_readahead={}",
        major, minor, init_in.max_readahead
    );

    if major != FUSE_KERNEL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported FUSE protocol version {}.{} (want {}.x)",
                major, minor, FUSE_KERNEL_VERSION
            ),
        ));
    }

    // Reconstruct 64-bit flags: flags | (flags2 << 32)
    let kernel_flags = (init_in.flags as u64) | ((init_in.flags2 as u64) << 32);
    debug!("kernel capabilities: 0x{:016x}", kernel_flags);

    if kernel_flags & FUSE_OVER_IO_URING == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kernel does not support FUSE_OVER_IO_URING (requires Linux 6.14+)",
        ));
    }

    Ok(ParsedFuseInit {
        unique,
        request,
        kernel_flags,
        kernel_max_readahead: init_in.max_readahead,
    })
}

/// Send the FUSE_INIT reply: intersect the kernel's capabilities with
/// what the mount options request, and use the filesystem's [`ReplyInit`]
/// for `max_write`, readahead, and background tuning.
fn write_fuse_init_reply(
    fuse_fd: BorrowedFd<'_>,
    parsed: &ParsedFuseInit,
    max_write: u32,
    reply: &ReplyInit,
    opts: &MountOptions,
) -> io::Result<()> {
    let kernel_flags = parsed.kernel_flags;

    // Build capability flags
    let mut want_flags: u64 = FUSE_OVER_IO_URING;
    want_flags |= FUSE_INIT_EXT as u64;
    want_flags |= FUSE_ASYNC_READ as u64;
    want_flags |= FUSE_BIG_WRITES as u64;
    want_flags |= FUSE_AUTO_INVAL_DATA as u64;
    want_flags |= FUSE_DO_READDIRPLUS as u64;
    want_flags |= FUSE_READDIRPLUS_AUTO as u64;
    want_flags |= FUSE_ASYNC_DIO as u64;
    want_flags |= FUSE_PARALLEL_DIROPS as u64;
    want_flags |= FUSE_MAX_PAGES as u64;
    want_flags |= FUSE_ATOMIC_O_TRUNC as u64;
    want_flags |= FUSE_SETXATTR_EXT as u64;

    // Lock advertisement is opt-in: enabling these flags routes all
    // fcntl/flock calls to userspace, where they ENOSYS unless the
    // Filesystem impl provides getlk/setlk/flock. Leaving them off lets
    // the kernel handle local-only locks.
    if opts.posix_locks {
        want_flags |= FUSE_POSIX_LOCKS as u64;
    }
    if opts.flock_locks {
        want_flags |= FUSE_FLOCK_LOCKS as u64;
    }

    if opts.dont_mask {
        want_flags |= FUSE_DONT_MASK as u64;
    }
    if opts.no_open_support {
        want_flags |= FUSE_NO_OPEN_SUPPORT as u64;
    }
    if opts.no_open_dir_support {
        want_flags |= FUSE_NO_OPENDIR_SUPPORT as u64;
    }
    if opts.handle_killpriv {
        want_flags |= FUSE_HANDLE_KILLPRIV as u64;
    }
    if opts.passthrough {
        // FUSE_PASSTHROUGH and FUSE_WRITEBACK_CACHE are mutually exclusive
        want_flags |= FUSE_PASSTHROUGH;
    } else if opts.write_back {
        want_flags |= FUSE_WRITEBACK_CACHE as u64;
    }

    // Only enable flags the kernel supports
    want_flags &= kernel_flags;

    if want_flags & FUSE_OVER_IO_URING == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "FUSE_OVER_IO_URING not supported after negotiation",
        ));
    }

    // The kernel proposes max_readahead; the FS can only lower it.
    let max_readahead = parsed.kernel_max_readahead.min(reply.max_readahead);

    let out_hdr = fuse_out_header {
        len: (std::mem::size_of::<fuse_out_header>() + std::mem::size_of::<fuse_init_out>()) as u32,
        error: 0,
        unique: parsed.unique,
    };

    let init_out = fuse_init_out {
        major: FUSE_KERNEL_VERSION,
        minor: FUSE_KERNEL_MINOR_VERSION,
        max_readahead,
        flags: (want_flags & 0xFFFF_FFFF) as u32,
        max_background: reply.max_background,
        congestion_threshold: reply.congestion_threshold,
        max_write,
        time_gran: 1,
        max_pages: (max_write / 4096).max(1) as u16,
        map_alignment: 0,
        flags2: ((want_flags >> 32) & 0xFFFF_FFFF) as u32,
        max_stack_depth: if opts.passthrough { 1 } else { 0 },
        request_timeout: 0,
        unused: [0; 11],
    };

    let hdr_bytes = unsafe {
        std::slice::from_raw_parts(
            &out_hdr as *const _ as *const u8,
            std::mem::size_of::<fuse_out_header>(),
        )
    };
    let body_bytes = unsafe {
        std::slice::from_raw_parts(
            &init_out as *const _ as *const u8,
            std::mem::size_of::<fuse_init_out>(),
        )
    };

    let mut response = Vec::with_capacity(hdr_bytes.len() + body_bytes.len());
    response.extend_from_slice(hdr_bytes);
    response.extend_from_slice(body_bytes);

    nix::unistd::write(fuse_fd, &response).map_err(io::Error::from)?;

    info!(
        "FUSE_INIT reply sent: flags=0x{:016x}, max_write={}",
        want_flags, max_write
    );

    Ok(())
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

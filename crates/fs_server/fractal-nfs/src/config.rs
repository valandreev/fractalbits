//! Runtime-agnostic server configuration.

/// Configuration for the NFS server, shared by both the compio and tokio
/// backends.
pub struct NfsServerConfig {
    /// NFS + MOUNT listen port (both on same port for simplicity).
    pub port: u16,
    /// Number of per-CPU listener threads. Each thread runs an independent
    /// runtime and binds to the port with SO_REUSEPORT, so the kernel
    /// load-balances incoming connections across them.
    pub num_threads: usize,
    /// FSID for file handles.
    pub fsid: u64,
}

impl Default for NfsServerConfig {
    fn default() -> Self {
        Self {
            port: 2049,
            num_threads: num_cpus(),
            fsid: 1,
        }
    }
}

pub(crate) fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Build a non-blocking, SO_REUSEPORT-enabled, listening `std::net::TcpListener`
/// for the given address. Each runtime backend wraps it into its own
/// listener type.
pub(crate) fn bind_reuseport_std(
    addr: std::net::SocketAddr,
) -> std::io::Result<std::net::TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(128)?;
    Ok(socket.into())
}

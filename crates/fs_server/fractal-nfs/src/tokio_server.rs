//! TCP server for NFSv3 with per-CPU tokio current-thread runtimes and
//! SO_REUSEPORT. Each thread runs an independent `current_thread` runtime
//! inside a `LocalSet` so per-connection tasks don't need `Send` futures --
//! that matters because the `Nfs3Filesystem` trait doesn't bound its
//! returned futures with `Send`.
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::Nfs3Filesystem;
use crate::config::{NfsServerConfig, bind_reuseport_std};
use crate::dispatch;
use crate::nfs3_types::NfsFh3;
use crate::rpc::{self, RpcCallHeader, frame_reply};
use crate::xdr::XdrReader;

/// Run the NFS server on per-CPU tokio current-thread runtimes. Blocks
/// until shutdown.
pub fn run<F: Nfs3Filesystem>(fs: F, config: NfsServerConfig) -> std::io::Result<()> {
    let fs = Arc::new(fs);
    let root_fh = NfsFh3::new(1, config.fsid); // inode 1 = root

    let addr: std::net::SocketAddr = ([0, 0, 0, 0], config.port).into();

    let mut handles = Vec::new();

    for thread_id in 0..config.num_threads {
        let fs = fs.clone();
        let root_fh = root_fh.clone();

        let handle = std::thread::Builder::new()
            .name(format!("nfs-{thread_id}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .build()
                    .expect("Failed to create tokio runtime for NFS thread");

                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    let listener = bind_reuseport_tokio(addr).expect("Failed to bind NFS listener");
                    tracing::info!(
                        thread = thread_id,
                        port = config.port,
                        "NFS listener started"
                    );

                    loop {
                        match listener.accept().await {
                            Ok((stream, peer)) => {
                                tracing::debug!(%peer, "NFS connection accepted");
                                let fs = fs.clone();
                                let root_fh = root_fh.clone();
                                tokio::task::spawn_local(async move {
                                    if let Err(e) = handle_connection(stream, &fs, &root_fh).await {
                                        tracing::debug!(%peer, error = %e, "NFS connection closed");
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "NFS accept error");
                            }
                        }
                    }
                });
            })?;

        handles.push(handle);
    }

    for h in handles {
        let _ = h.join();
    }

    Ok(())
}

fn bind_reuseport_tokio(addr: std::net::SocketAddr) -> std::io::Result<TcpListener> {
    let std_listener = bind_reuseport_std(addr)?;
    TcpListener::from_std(std_listener)
}

async fn handle_connection<F: Nfs3Filesystem>(
    mut stream: TcpStream,
    fs: &Arc<F>,
    root_fh: &NfsFh3,
) -> std::io::Result<()> {
    let mut recv_buf = BytesMut::with_capacity(64 * 1024);
    let mut tmp = vec![0u8; 32 * 1024];

    loop {
        // Read TCP record mark (4 bytes).
        while recv_buf.len() < 4 {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Ok(()); // Connection closed
            }
            recv_buf.extend_from_slice(&tmp[..n]);
        }

        let (frag_len, _last) = rpc::read_record_mark(&recv_buf)
            .ok_or_else(|| std::io::Error::other("invalid record mark"))?;
        let total_needed = 4 + frag_len as usize;

        while recv_buf.len() < total_needed {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(std::io::Error::other("connection closed mid-fragment"));
            }
            recv_buf.extend_from_slice(&tmp[..n]);
        }

        let msg_data = recv_buf.split_to(total_needed);
        let payload = &msg_data[4..];

        let mut reader = XdrReader::new(payload);
        let header = match RpcCallHeader::decode(&mut reader) {
            Ok(h) => h,
            Err(_) => {
                tracing::debug!("Failed to decode RPC header, skipping");
                continue;
            }
        };

        let args_buf = reader.read_remaining();
        let reply_body = dispatch::dispatch_rpc(fs, &header, args_buf, root_fh).await;
        let reply_frame = frame_reply(&reply_body);

        stream.write_all(&reply_frame).await?;
    }
}

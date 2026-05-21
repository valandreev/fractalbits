#![doc = include_str!("../README.md")]

pub mod config;
pub mod dispatch;
pub mod mount;
pub mod nfs3_types;
pub mod nfs3_wire;
pub mod rpc;
pub mod xdr;

#[cfg(feature = "compio-runtime")]
pub mod compio_server;
#[cfg(feature = "tokio-runtime")]
pub mod tokio_server;

pub use config::NfsServerConfig;
pub use nfs3_types::*;

/// Re-export `run` when exactly one backend is enabled. With both enabled,
/// callers select explicitly via `compio_server::run` / `tokio_server::run`.
#[cfg(all(feature = "compio-runtime", not(feature = "tokio-runtime")))]
pub use compio_server::run;
#[cfg(all(feature = "tokio-runtime", not(feature = "compio-runtime")))]
pub use tokio_server::run;

/// Result type for NFS operations: Ok(()) means the success response was
/// already encoded into the XdrWriter; Err(status) is encoded by the
/// dispatch layer so filesystem code only needs to return the error code.
pub type NfsResult = Result<(), Nfsstat3>;

/// Trait for implementing an NFSv3 filesystem.
///
/// Each method receives pre-decoded arguments and an XdrWriter for encoding
/// the response body (status + result data). The RPC accepted header is
/// already written before these methods are called.
///
/// On success, methods encode the OK response into the writer and return
/// `Ok(())`. On failure, methods return `Err(Nfsstat3::...)` and the
/// dispatch layer encodes the error response (truncating any partial writes).
///
/// All methods are async and !Send (compio single-threaded model).
/// The trait itself is Send + Sync for Arc sharing across threads.
pub trait Nfs3Filesystem: Send + Sync + 'static {
    fn getattr(
        &self,
        fh: &NfsFh3,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult>;

    fn setattr(
        &self,
        fh: &NfsFh3,
        attrs: &Sattr3,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult> {
        let _ = (fh, attrs, w);
        async { Err(Nfsstat3::NotSupp) }
    }

    fn lookup(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult>;

    fn access(
        &self,
        fh: &NfsFh3,
        access: u32,
        uid: u32,
        gid: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult>;

    fn readlink(
        &self,
        fh: &NfsFh3,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult> {
        let _ = (fh, w);
        async { Err(Nfsstat3::Inval) }
    }

    fn read(
        &self,
        fh: &NfsFh3,
        offset: u64,
        count: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult>;

    fn write(
        &self,
        fh: &NfsFh3,
        offset: u64,
        data: &[u8],
        stable: StableHow,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult> {
        let _ = (fh, offset, data, stable, w);
        async { Err(Nfsstat3::Rofs) }
    }

    fn create(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult> {
        let _ = (dir_fh, name, w);
        async { Err(Nfsstat3::Rofs) }
    }

    fn mkdir(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult> {
        let _ = (dir_fh, name, w);
        async { Err(Nfsstat3::Rofs) }
    }

    fn remove(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult> {
        let _ = (dir_fh, name, w);
        async { Err(Nfsstat3::Rofs) }
    }

    fn rmdir(
        &self,
        dir_fh: &NfsFh3,
        name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult> {
        let _ = (dir_fh, name, w);
        async { Err(Nfsstat3::Rofs) }
    }

    fn rename(
        &self,
        from_dir: &NfsFh3,
        from_name: &str,
        to_dir: &NfsFh3,
        to_name: &str,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult> {
        let _ = (from_dir, from_name, to_dir, to_name, w);
        async { Err(Nfsstat3::Rofs) }
    }

    fn readdir(
        &self,
        dir_fh: &NfsFh3,
        cookie: u64,
        count: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult>;

    fn readdirplus(
        &self,
        dir_fh: &NfsFh3,
        cookie: u64,
        maxcount: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult>;

    fn fsstat(
        &self,
        fh: &NfsFh3,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult>;

    fn fsinfo(
        &self,
        fh: &NfsFh3,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult>;

    fn pathconf(
        &self,
        fh: &NfsFh3,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult>;

    fn commit(
        &self,
        fh: &NfsFh3,
        offset: u64,
        count: u32,
        w: &mut xdr::XdrWriter,
    ) -> impl std::future::Future<Output = NfsResult> {
        let _ = (fh, offset, count, w);
        async { Err(Nfsstat3::NotSupp) }
    }
}

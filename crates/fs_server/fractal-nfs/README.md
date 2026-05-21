# fractal-nfs

[![CI](https://github.com/fractalbits-labs/fractalbits-main/actions/workflows/ci.yml/badge.svg)](https://github.com/fractalbits-labs/fractalbits-main/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/fractal-nfs.svg)](https://crates.io/crates/fractal-nfs)
[![docs.rs](https://docs.rs/fractal-nfs/badge.svg)](https://docs.rs/fractal-nfs)
[![source](https://img.shields.io/badge/source-GitHub-blue)](https://github.com/fractalbits-labs/fractalbits-main/tree/main/crates/fs_server/fractal-nfs)

A minimal, embeddable **NFSv3** server in Rust. Implement one trait
(`Nfs3Filesystem`), pick an async runtime (tokio by default, compio
optional), call `run(...)`, and your filesystem is reachable over NFS.

The crate handles ONC RPC framing, XDR encoding/decoding, the MOUNT
protocol, and NFSv3 procedure dispatch. Filesystem code only deals with
pre-decoded arguments and writes typed responses through helpers.

## Features

- **Runtime-agnostic** -- pick `tokio-runtime` (default) or
  `compio-runtime` via cargo features; both can be enabled at once for
  multi-target consumers
- **Per-CPU listener threads** with `SO_REUSEPORT` so the kernel
  load-balances connections; each thread runs an independent
  single-threaded runtime so `Nfs3Filesystem` futures don't need `Send`
- **Single-trait surface** -- implement `Nfs3Filesystem` (async methods,
  most with sensible defaults like `Rofs` or `NotSupp`) and you're done
- **Read-only and read-write** -- defaults make read-only filesystems a
  one-liner; override `write`/`create`/`mkdir`/etc. when you need them
- **Symlink support** -- includes `readlink` for filesystems that
  surface symbolic links
- **MOUNT v3 included** -- single TCP port for both NFS and MOUNT so no
  separate mountd process is needed
- **No portmapper required** -- clients mount with explicit
  `port=N,mountport=N` options

## Requirements

- **Rust edition 2024** (Rust 1.85+)
- An NFSv3 client (Linux `mount -t nfs`, macOS, etc.) -- the server is
  cross-platform but only Linux/macOS are exercised in CI

## Usage

Implement `Nfs3Filesystem` and call `run`:

```rust,no_run
use fractal_nfs::{
    Nfs3Filesystem, NfsFh3, NfsResult, NfsServerConfig, Nfsstat3,
    nfs3_wire, xdr::XdrWriter,
};

struct MyFs;

impl Nfs3Filesystem for MyFs {
    async fn getattr(&self, _fh: &NfsFh3, _w: &mut XdrWriter) -> NfsResult {
        Err(Nfsstat3::NotSupp)
    }
    async fn lookup(&self, _dir: &NfsFh3, _name: &str, _w: &mut XdrWriter) -> NfsResult {
        Err(Nfsstat3::Noent)
    }
    async fn access(&self, _fh: &NfsFh3, _a: u32, _u: u32, _g: u32, _w: &mut XdrWriter) -> NfsResult {
        Err(Nfsstat3::NotSupp)
    }
    async fn read(&self, _fh: &NfsFh3, _o: u64, _c: u32, _w: &mut XdrWriter) -> NfsResult {
        Err(Nfsstat3::NotSupp)
    }
    async fn readdir(&self, _fh: &NfsFh3, _c: u64, _n: u32, _w: &mut XdrWriter) -> NfsResult {
        Err(Nfsstat3::NotSupp)
    }
    async fn readdirplus(&self, _fh: &NfsFh3, _c: u64, _m: u32, _w: &mut XdrWriter) -> NfsResult {
        Err(Nfsstat3::NotSupp)
    }
    async fn fsstat(&self, _fh: &NfsFh3, _w: &mut XdrWriter) -> NfsResult {
        Err(Nfsstat3::NotSupp)
    }
    async fn fsinfo(&self, _fh: &NfsFh3, _w: &mut XdrWriter) -> NfsResult {
        Err(Nfsstat3::NotSupp)
    }
    async fn pathconf(&self, _fh: &NfsFh3, _w: &mut XdrWriter) -> NfsResult {
        Err(Nfsstat3::NotSupp)
    }
}

fn main() -> std::io::Result<()> {
    let cfg = NfsServerConfig {
        port: 2049,
        num_threads: 4,
        fsid: 1,
    };
    fractal_nfs::run(MyFs, cfg)
}
```

Then mount from a client:

```sh
sudo mount -o port=2049,mountport=2049,vers=3,tcp,nolock -t nfs localhost:/ /mnt/myfs
```

## Architecture

```text
NfsServerConfig { port, num_threads, fsid }
        |
        +-- bind_reuseport_std(addr)  (SO_REUSEPORT listening socket)
        +-- N x std::thread::spawn
              |
              +-- runtime::block_on(...)
                    |
                    +-- listener.accept().await
                          |
                          +-- spawn(handle_connection(stream, &fs, &root_fh))
                                |
                                +-- read TCP record mark (4 bytes)
                                +-- read fragment payload
                                +-- decode RPC call header
                                +-- dispatch::dispatch_rpc(fs, header, args, root_fh)
                                |     |
                                |     +-- MOUNT program -> mount::handle_mount_call
                                |     +-- NFS program -> NFSPROC3_* -> trait method
                                +-- frame_reply(reply_body) (prepend record mark)
                                +-- stream.write_all(reply_frame)
```

Each listener thread owns its own single-threaded runtime (`compio::Runtime`
or `tokio::current_thread + LocalSet`). All threads share an `Arc<F>` so
the filesystem implementation must be `Send + Sync + 'static`, but
returned futures don't need `Send` -- handy when the filesystem holds
non-`Send` handles.

## Nfs3Filesystem Trait

Implement
[`Nfs3Filesystem`](https://docs.rs/fractal-nfs/latest/fractal_nfs/trait.Nfs3Filesystem.html)
for your filesystem. Most methods have a sensible default
(`Err(Nfsstat3::Rofs)` for write operations, `Err(Nfsstat3::NotSupp)` for
optional ones, `Err(Nfsstat3::Inval)` for `readlink`), so a read-only
filesystem only needs to implement the read-side ops.

Operations are split below by category. The "Default" column shows the
error returned if the method isn't overridden.

### Required (no default)

| Operation | Description |
|-----------|-------------|
| `getattr` | Get file attributes |
| `lookup` | Look up a directory entry by name |
| `access` | Check access rights |
| `read` | Read file data |
| `readdir` | Read directory entries |
| `readdirplus` | Read directory entries with attributes |
| `fsstat` | Filesystem space + inode statistics |
| `fsinfo` | Static filesystem info (block sizes, limits) |
| `pathconf` | POSIX `pathconf`-style limits |

### Optional (defaults supplied)

| Operation | Default | Description |
|-----------|---------|-------------|
| `setattr` | `NotSupp` | Set file attributes |
| `readlink` | `Inval` | Read a symbolic link target |
| `write` | `Rofs` | Write file data |
| `create` | `Rofs` | Create a regular file |
| `mkdir` | `Rofs` | Create a directory |
| `remove` | `Rofs` | Unlink a file |
| `rmdir` | `Rofs` | Remove an empty directory |
| `rename` | `Rofs` | Rename a file or directory |
| `commit` | `NotSupp` | Commit unstable writes (no-op when writes are FILE_SYNC) |

Each method receives an `XdrWriter` and, on success, encodes the OK
response using helpers from
[`nfs3_wire`](https://docs.rs/fractal-nfs/latest/fractal_nfs/nfs3_wire/index.html)
(e.g. `encode_getattr_ok`, `encode_lookup_ok`). On error, return
`Err(Nfsstat3::...)` and the dispatch layer encodes the failure response
for you.

## Features

| Feature | Default | Pulls in | Notes |
|---------|---------|----------|-------|
| `tokio-runtime` | yes | `tokio` (`rt`, `net`, `io-util`) | Per-CPU `current_thread` runtimes + `LocalSet` |
| `compio-runtime` | no | `compio-runtime`, `compio-net`, `compio-io`, `compio-buf` | Per-CPU compio runtimes; integrates with crates already using compio |

With exactly one runtime feature enabled, `fractal_nfs::run` resolves to
that backend. With both enabled, callers pick explicitly via
`fractal_nfs::tokio_server::run` or `fractal_nfs::compio_server::run`.

## License

Licensed under [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

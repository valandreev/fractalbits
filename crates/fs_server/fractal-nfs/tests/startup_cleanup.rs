//! Verify the server returns deterministically from a startup failure
//! instead of hanging on workers that never received a shutdown wakeup.

use std::net::TcpListener;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use fractal_nfs::{Nfs3Filesystem, NfsServerConfig};

struct NullFs;
impl Nfs3Filesystem for NullFs {}

/// Claim an ephemeral port with a plain `std::net::TcpListener`. By
/// default this listener has SO_REUSEPORT unset; Linux only allows a
/// SO_REUSEPORT bind on a port if every prior binder also set the
/// option, so the subsequent SO_REUSEPORT bind from `fractal_nfs::run`
/// on the same port will fail with AddrInUse -- which is exactly what
/// the tests below exercise.
fn occupy_port() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().expect("local_addr").port();
    (listener, port)
}

#[test]
fn rejects_num_threads_zero() {
    let cfg = NfsServerConfig {
        port: 0,
        num_threads: 0,
        ..Default::default()
    };
    let err = fractal_nfs::run(NullFs, cfg)
        .expect_err("num_threads=0 must be rejected, not silently succeed");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::InvalidInput,
        "expected InvalidInput, got {err:?}"
    );
}

#[test]
fn returns_quickly_on_bind_failure() {
    let (_blocker, port) = occupy_port();

    let cfg = NfsServerConfig {
        port,
        num_threads: 4,
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel();
    let t = std::thread::spawn(move || {
        let result = fractal_nfs::run(NullFs, cfg);
        let _ = tx.send(result);
    });

    // The bind happens synchronously on the main thread inside run(),
    // before any worker is spawned, so this must return very quickly.
    // Give it a comfortable upper bound for slow CI hosts.
    let result = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("run() should return within 5s on bind failure, not hang");
    let err = result.expect_err("run() should return Err when the port is already taken");
    assert!(
        matches!(
            err.kind(),
            std::io::ErrorKind::AddrInUse | std::io::ErrorKind::PermissionDenied
        ),
        "expected AddrInUse/PermissionDenied, got {err:?}"
    );

    t.join().expect("run() thread");
}

#[test]
fn shuts_down_when_some_workers_started_then_bind_fails() {
    let (_blocker, port) = occupy_port();
    let start = Instant::now();
    let cfg = NfsServerConfig {
        port,
        num_threads: 8,
        ..Default::default()
    };
    let err = fractal_nfs::run(NullFs, cfg).expect_err("port is taken; run() must return Err");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "run() should fail quickly, took {elapsed:?}; error was {err:?}"
    );
}

//! Test overflow configuration on Connection::Builder.
//!
//! Verifies that `overflow(true)` prevents the Single Point of Failure where a
//! full signal broadcast channel blocks the entire D-Bus connection.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{StreamExt, future::Either, pin_mut};
use ntest::timeout;
use test_log::test;
use zbus::{Connection, connection, interface, object_server::InterfaceRef};

struct OverflowTestIface;

#[interface(name = "org.zbus.test.Overflow")]
impl OverflowTestIface {
    #[zbus(signal)]
    async fn state_changed(
        emitter: &zbus::object_server::SignalEmitter<'_>,
        state: &str,
    ) -> zbus::Result<()>;
}

#[zbus::proxy(interface = "org.zbus.test.Overflow", assume_defaults = true)]
trait OverflowTest {
    #[zbus(signal)]
    async fn state_changed(&self, state: &str) -> zbus::Result<()>;
}

async fn setup_server() -> zbus::Result<(Connection, InterfaceRef<OverflowTestIface>)> {
    let conn = connection::Builder::session()?
        .serve_at("/org/zbus/test/Overflow", OverflowTestIface)?
        .name("org.zbus.test.Overflow")?
        .build()
        .await?;

    let iface_ref = conn
        .object_server()
        .interface::<_, OverflowTestIface>("/org/zbus/test/Overflow")
        .await?;

    Ok((conn, iface_ref))
}

/// Emit signals from a separate thread (server-side) to avoid executor conflicts.
fn emit_signals(iface_ref: InterfaceRef<OverflowTestIface>, count: u32) {
    std::thread::spawn(move || {
        zbus::block_on(async move {
            for i in 0..count {
                let emitter = iface_ref.signal_emitter();
                let _ = OverflowTestIface::state_changed(emitter, &format!("State{}", i)).await;
            }
        });
    });
}

fn emit_one_signal(iface_ref: InterfaceRef<OverflowTestIface>, state: &str) {
    let state = state.to_string();
    std::thread::spawn(move || {
        zbus::block_on(async move {
            let emitter = iface_ref.signal_emitter();
            let _ = OverflowTestIface::state_changed(emitter, &state).await;
        });
    });
}

/// Basic sanity: a signal can be received without overflow.
#[test]
#[timeout(15000)]
fn basic_signal_reception() {
    let result = Arc::new(Mutex::new(false));
    let result_clone = result.clone();

    zbus::block_on(async {
        let (server_conn, iface_ref) = setup_server().await.unwrap();
        let client_conn = connection::Builder::session()
            .unwrap()
            .build()
            .await
            .unwrap();

        let server_name = server_conn.unique_name().unwrap().clone();
        let r = result_clone;

        let task = client_conn.clone().executor().spawn(
            async move {
                let proxy = OverflowTestProxy::builder(&client_conn)
                    .destination(server_name)
                    .unwrap()
                    .path("/org/zbus/test/Overflow")
                    .unwrap()
                    .build()
                    .await
                    .unwrap();

                let mut stream = proxy.receive_state_changed().await.unwrap();

                emit_one_signal(iface_ref, "Hello");

                let signal = stream.next().await;
                *r.lock().unwrap() = signal.is_some();
            },
            "signal_test",
        );

        let timer = async_io::Timer::after(Duration::from_secs(5));
        pin_mut!(timer);
        let task_result = futures_util::future::select(timer, task).await;

        drop(server_conn);

        if let Either::Right((_, _)) = task_result {
            assert!(*result.lock().unwrap(), "Should receive the signal");
        } else {
            panic!("Test timed out");
        }
    });
}

/// With overflow=false (default), the connection freezes when a signal buffer
/// is full. Subscribing to a new signal times out because the socket_reader
/// cannot read the AddMatch response.
#[test]
#[timeout(15000)]
fn overflow_false_freezes_connection() {
    let result = Arc::new(Mutex::new(true));
    let result_clone = result.clone();

    zbus::block_on(async {
        let (server_conn, iface_ref) = setup_server().await.unwrap();
        let client_conn = connection::Builder::session()
            .unwrap()
            .build()
            .await
            .unwrap();

        let server_name = server_conn.unique_name().unwrap().clone();
        let r = result_clone;

        let task = client_conn.clone().executor().spawn(
            async move {
                let proxy = OverflowTestProxy::builder(&client_conn)
                    .destination(server_name)
                    .unwrap()
                    .path("/org/zbus/test/Overflow")
                    .unwrap()
                    .build()
                    .await
                    .unwrap();

                // Subscribe to signal but do NOT consume
                let _stream_a = proxy.receive_state_changed().await.unwrap();

                // Emit 65 signals to fill the 64-slot buffer + 1
                emit_signals(iface_ref.clone(), 65);

                async_io::Timer::after(Duration::from_millis(500)).await;

                // Try to subscribe again with a 2s timeout
                let timer = async_io::Timer::after(Duration::from_secs(2));
                let sub = proxy.receive_state_changed();
                pin_mut!(timer);
                pin_mut!(sub);

                let sub_result = match futures_util::future::select(timer, sub).await {
                    Either::Left((_, _)) => None,
                    Either::Right((result, _)) => Some(result),
                };

                let mut is_frozen = true;
                if let Some(Ok(mut stream_b)) = sub_result {
                    // add_match succeeded — try to receive a signal
                    emit_one_signal(iface_ref, "ShouldNotArrive");

                    let timer = async_io::Timer::after(Duration::from_millis(500));
                    let signal = stream_b.next();
                    pin_mut!(timer);
                    pin_mut!(signal);

                    let result = match futures_util::future::select(timer, signal).await {
                        Either::Left((_, _)) => None,
                        Either::Right((msg, _)) => msg,
                    };
                    is_frozen = result.is_none();
                }

                *r.lock().unwrap() = is_frozen;
            },
            "overflow_false_test",
        );

        let timer = async_io::Timer::after(Duration::from_secs(10));
        pin_mut!(timer);
        let task_result = futures_util::future::select(timer, task).await;

        drop(server_conn);

        if let Either::Right((_, _)) = task_result {
            assert!(
                *result.lock().unwrap(),
                "Connection should be frozen with overflow=false"
            );
        } else {
            panic!("Test timed out");
        }
    });
}

/// With overflow=true, subscribing to a second signal still works after the first
/// signal's buffer is full. The connection is NOT frozen.
#[test]
#[timeout(15000)]
fn overflow_true_allows_new_subscriptions() {
    let result = Arc::new(Mutex::new(false));
    let result_clone = result.clone();

    zbus::block_on(async {
        let (server_conn, iface_ref) = setup_server().await.unwrap();
        let client_conn = connection::Builder::session()
            .unwrap()
            .overflow(true)
            .build()
            .await
            .unwrap();

        let server_name = server_conn.unique_name().unwrap().clone();
        let r = result_clone;

        let task = client_conn.clone().executor().spawn(
            async move {
                let proxy = OverflowTestProxy::builder(&client_conn)
                    .destination(server_name)
                    .unwrap()
                    .path("/org/zbus/test/Overflow")
                    .unwrap()
                    .build()
                    .await
                    .unwrap();

                // Subscribe to signal but do NOT consume the stream
                let _stream_a = proxy.receive_state_changed().await.unwrap();

                // Emit 70 signals (exceeds default capacity of 64)
                emit_signals(iface_ref.clone(), 70);

                async_io::Timer::after(Duration::from_millis(500)).await;

                // Connection should NOT be frozen: can subscribe again
                let mut stream_b = proxy.receive_state_changed().await.unwrap();

                // Emit one more signal
                emit_one_signal(iface_ref, "FreshState");

                // The fresh signal should arrive within 2s
                let timer = async_io::Timer::after(Duration::from_secs(2));
                let signal = stream_b.next();
                pin_mut!(timer);
                pin_mut!(signal);

                let signal_result = match futures_util::future::select(timer, signal).await {
                    Either::Left((_, _)) => None,
                    Either::Right((msg, _)) => msg,
                };

                *r.lock().unwrap() = signal_result.is_some();
            },
            "overflow_true_test",
        );

        let timer = async_io::Timer::after(Duration::from_secs(10));
        pin_mut!(timer);
        let task_result = futures_util::future::select(timer, task).await;

        drop(server_conn);

        if let Either::Right((_, _)) = task_result {
            assert!(
                *result.lock().unwrap(),
                "Connection should not be frozen with overflow=true"
            );
        } else {
            panic!("Test timed out");
        }
    });
}

/// With overflow=true, the channel drops the oldest messages when full.
/// After emitting 100 signals into a 64-slot buffer, the receiver gets
/// the most recent ~64 messages (oldest ~36 are dropped).
#[test]
#[timeout(15000)]
fn overflow_true_drops_oldest_messages() {
    let count_result = Arc::new(Mutex::new((0u32, String::new(), String::new())));
    let count_clone = count_result.clone();

    zbus::block_on(async {
        let (server_conn, iface_ref) = setup_server().await.unwrap();
        let client_conn = connection::Builder::session()
            .unwrap()
            .overflow(true)
            .build()
            .await
            .unwrap();

        let server_name = server_conn.unique_name().unwrap().clone();
        let cr = count_clone;

        let task = client_conn.clone().executor().spawn(
            async move {
                let proxy = OverflowTestProxy::builder(&client_conn)
                    .destination(server_name)
                    .unwrap()
                    .path("/org/zbus/test/Overflow")
                    .unwrap()
                    .build()
                    .await
                    .unwrap();

                let mut stream = proxy.receive_state_changed().await.unwrap();

                // Emit 100 signals with distinct values
                emit_signals(iface_ref, 100);

                async_io::Timer::after(Duration::from_millis(500)).await;

                // Drain available signals
                let mut count = 0u32;
                let mut first_state = String::new();
                let mut last_state = String::new();

                loop {
                    let timer = async_io::Timer::after(Duration::from_millis(100));
                    let signal = stream.next();
                    pin_mut!(timer);
                    pin_mut!(signal);

                    match futures_util::future::select(timer, signal).await {
                        Either::Left((_, _)) => break,
                        Either::Right((Some(signal), _)) => {
                            let args = signal.args().unwrap();
                            if count == 0 {
                                first_state = args.state.to_string();
                            }
                            last_state = args.state.to_string();
                            count += 1;
                        }
                        Either::Right((None, _)) => break,
                    }
                }

                *cr.lock().unwrap() = (count, first_state, last_state);
            },
            "overflow_drops_test",
        );

        let timer = async_io::Timer::after(Duration::from_secs(10));
        pin_mut!(timer);
        let task_result = futures_util::future::select(timer, task).await;

        drop(server_conn);

        if let Either::Right((_, _)) = task_result {
            let guard = count_result.lock().unwrap();
            let (count, first_state, last_state) = guard.clone();

            assert!(
                count >= 60,
                "Should receive approximately 64 messages (buffer size), got {}",
                count
            );

            assert!(
                !first_state.contains("State0"),
                "First received should NOT be State0 — oldest messages should be dropped"
            );

            assert!(
                last_state.contains("State9"),
                "Last received should be near State99 — newest messages should be kept, got {}",
                last_state
            );
        } else {
            panic!("Test timed out");
        }
    });
}

#![cfg(unix)]
#![allow(unsafe_code)]

use std::mem::MaybeUninit;
use std::sync::mpsc;
use std::time::Duration;

use dst::Builder;
use dst::os_hooks::{ClockGuard, clock_test_lock, publish_sim_elapsed, set_os_rng_from_seed};

/// Default wall epoch (`DEFAULT_WALL_CLOCK_EPOCH`) in nanoseconds — E2.
const EPOCH_NS: u64 = 1_700_000_000 * 1_000_000_000;

fn read_clock(clockid: libc::clockid_t) -> libc::timespec {
    let mut ts = MaybeUninit::<libc::timespec>::uninit();
    let rc = unsafe { libc::clock_gettime(clockid, ts.as_mut_ptr()) };
    assert_eq!(rc, 0);
    unsafe { ts.assume_init() }
}

fn timespec_nanos(ts: libc::timespec) -> u64 {
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

/// The `getrandom` crate often resolves libc before our `#[no_mangle]` symbol; hook behavior
/// for the shared mutex path is covered by `os_hooks::rand` unit tests when built with `os-hooks`.
#[test]
fn getrandom_crate_still_works_with_hooks_enabled() {
    set_os_rng_from_seed(7);
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf).expect("getrandom");
}

#[test]
fn clock_hook_delegates_to_os_outside_dst_context() {
    // C6: use the framework-provided process-wide lock at every init site.
    let _lock = clock_test_lock();
    let _guard = ClockGuard::init();
    set_os_rng_from_seed(0);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("runtime");
    let _enter = rt.enter();

    // Standalone-callable without a Sim (required: hook must still delegate).
    publish_sim_elapsed(Duration::from_millis(7));

    // Outside an active dst node context, the hook delegates to the real OS
    // clock (R6 gate is `in_node_context()`), so we see real wall time.
    let realtime = read_clock(libc::CLOCK_REALTIME);
    assert_ne!(realtime.tv_sec, 0);
    assert_ne!(timespec_nanos(realtime), 7_000_000);
}

#[test]
fn sim_step_publishes_elapsed_for_hooks() {
    let _lock = clock_test_lock();
    let _guard = ClockGuard::init();
    set_os_rng_from_seed(1);

    let mut sim = Builder::new()
        .rng_seed(99)
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(10))
        .build();

    sim.client("c", async { Ok(()) }).expect("client");

    sim.step().expect("step");
    // R8: registering the client folds INIT_ALIGN (1ms) into sim-global time
    // (was 0 before R8), then one 1ms step → 2ms total. (This is the
    // os_hooks_integration.rs:71 expectation the Burhan review flagged.)
    assert_eq!(sim.elapsed(), Duration::from_millis(2));
}

#[test]
fn clock_hook_uses_sim_time_inside_dst_context() {
    let _lock = clock_test_lock();
    let _guard = ClockGuard::init();
    set_os_rng_from_seed(2);

    let (tx, rx) = mpsc::channel();
    let mut sim = Builder::new()
        .rng_seed(99)
        .tick_duration(Duration::from_millis(10))
        .simulation_duration(Duration::from_secs(1))
        .build();

    sim.client("c", async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.send((
            read_clock(libc::CLOCK_REALTIME),
            read_clock(libc::CLOCK_MONOTONIC),
        ))
        .expect("send observed clocks");
        Ok(())
    })
    .expect("client");

    let mut observed = None;
    for _ in 0..100 {
        sim.step().expect("step");
        if let Ok(clock_pair) = rx.try_recv() {
            observed = Some(clock_pair);
            break;
        }
    }
    let (realtime, monotonic) = observed.expect("client should observe clocks");

    // E2: REALTIME and MONOTONIC are now DISTINCT sources.
    let mono_ns = timespec_nanos(monotonic);
    let real_ns = timespec_nanos(realtime);
    assert!(
        mono_ns > 0 && mono_ns <= 1_000_000_000,
        "monotonic={mono_ns}"
    );
    assert_eq!(
        real_ns,
        EPOCH_NS + mono_ns,
        "REALTIME must equal wall epoch + elapsed (E2)"
    );
    assert_ne!(
        real_ns, mono_ns,
        "E2: REALTIME must differ from MONOTONIC (was the bug)"
    );
}

/// C2/C7: process-global clock state is reset at each sim's construction, so a
/// second sequential sim in the same process does NOT observe the first sim's
/// (large) elapsed — no backward / cross-sim clock jump.
#[test]
fn sequential_sims_clock_resets_monotonic() {
    let _lock = clock_test_lock();
    let _guard = ClockGuard::init();
    set_os_rng_from_seed(3);

    fn first_observed_monotonic(seed: u64, run_steps: usize) -> u64 {
        let (tx, rx) = mpsc::channel();
        let mut sim = Builder::new()
            .rng_seed(seed)
            .tick_duration(Duration::from_millis(10))
            .simulation_duration(Duration::from_secs(5))
            .build();
        sim.client("c", async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            tx.send(read_clock(libc::CLOCK_MONOTONIC)).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(())
        })
        .expect("client");
        let mut first = None;
        for _ in 0..run_steps {
            sim.step().expect("step");
            if first.is_none()
                && let Ok(ts) = rx.try_recv()
            {
                first = Some(timespec_nanos(ts));
            }
        }
        let _ = sim.run();
        first.expect("client observed monotonic")
    }

    // First sim runs long (accumulates large elapsed). Second sim, freshly
    // constructed, must see a SMALL first monotonic — proving C2 reset, not
    // the prior sim's leftover elapsed.
    let _long = first_observed_monotonic(1, 100);
    let second = first_observed_monotonic(2, 100);
    assert!(
        second <= 100_000_000,
        "second sim leaked prior elapsed: {second}ns (clock did not reset)"
    );
}

/// C1: the alignment-sleep offset is published at registration, so the clock
/// atomic is consistent with `sim.elapsed()` before the first tick (a later
/// node's `spawn_and_init` clock read would otherwise observe a stale value).
#[test]
fn multi_node_registration_clock_published() {
    let _lock = clock_test_lock();
    let _guard = ClockGuard::init();
    set_os_rng_from_seed(4);

    let mut sim = Builder::new()
        .rng_seed(99)
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(5))
        .build();

    sim.host("h1", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .unwrap();
    sim.host("h2", || async {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .unwrap();
    sim.client("c", async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    })
    .unwrap();

    // 3 registrations × INIT_ALIGN(1ms), each republished (C1), before any
    // step: the published atomic tracks ctx.elapsed exactly.
    assert_eq!(sim.elapsed(), Duration::from_millis(3));
    assert_eq!(
        dst::os_hooks::sim_elapsed(),
        Some(Duration::from_millis(3)),
        "C1: publish-after-bump must keep the clock atomic in sync at registration"
    );
}

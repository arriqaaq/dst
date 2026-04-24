
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::sim::context::TickContext;

static USE_SIM_CLOCKS: AtomicUsize = AtomicUsize::new(0);

static ACTIVE_SIM_THREAD: AtomicU64 = AtomicU64::new(0);

static SIM_ELAPSED_NS: AtomicU64 = AtomicU64::new(0);

static SIM_WALL_EPOCH_NS: AtomicU64 = AtomicU64::new(0);

static CLOCK_TEST_LOCK: Mutex<()> = Mutex::new(());

pub fn clock_test_lock() -> MutexGuard<'static, ()> {
    CLOCK_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn thread_token() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut h);
    match h.finish() {
        0 => 1,
        t => t,
    }
}

pub fn publish_sim_elapsed(elapsed: Duration) {
    let ns = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
    SIM_ELAPSED_NS.store(ns, Ordering::Release);
}

pub fn set_sim_wall_epoch(epoch: Duration) {
    let ns = epoch.as_nanos().min(u128::from(u64::MAX)) as u64;
    SIM_WALL_EPOCH_NS.store(ns, Ordering::Release);
}

pub fn reset_sim_clock_state(epoch: Duration) {
    SIM_ELAPSED_NS.store(0, Ordering::Release);
    set_sim_wall_epoch(epoch);
}

pub(super) fn sim_elapsed_ns() -> u64 {
    SIM_ELAPSED_NS.load(Ordering::Acquire)
}

fn sim_wall_epoch_ns() -> u64 {
    SIM_WALL_EPOCH_NS.load(Ordering::Acquire)
}

fn ns_to_timespec(ns: u64) -> libc::timespec {
    libc::timespec {
        tv_sec: (ns / 1_000_000_000) as libc::time_t,
        tv_nsec: (ns % 1_000_000_000) as libc::c_long,
    }
}

pub struct ClockGuard(());

impl ClockGuard {
    pub fn init() -> Self {
        let me = thread_token();
        let prev = USE_SIM_CLOCKS.fetch_add(1, Ordering::AcqRel);
        if prev == 0 {
            ACTIVE_SIM_THREAD.store(me, Ordering::Release);
        } else {
            let owner = ACTIVE_SIM_THREAD.load(Ordering::Acquire);
            if owner != me {
                USE_SIM_CLOCKS.fetch_sub(1, Ordering::AcqRel);
                panic!(
                    "dst os-clock-hooks: concurrent ClockGuard::init from a \
                     different thread (owner {owner:#x}, this {me:#x}). The \
                     interposed clock_gettime symbol is process-global — \
                     serialize sims with \
                     dst_framework::os_hooks::clock_test_lock() or run them in \
                     separate processes."
                );
            }
        }
        Self(())
    }
}

impl Drop for ClockGuard {
    fn drop(&mut self) {
        if USE_SIM_CLOCKS.fetch_sub(1, Ordering::AcqRel) == 1 {
            ACTIVE_SIM_THREAD.store(0, Ordering::Release);
        }
    }
}

type ClockGettimeFn = unsafe extern "C" fn(libc::clockid_t, *mut libc::timespec) -> libc::c_int;

fn real_clock_gettime() -> Option<ClockGettimeFn> {
    static REAL_CLOCK_GETTIME: OnceLock<Option<ClockGettimeFn>> = OnceLock::new();
    *REAL_CLOCK_GETTIME.get_or_init(|| {
        let symbol = b"clock_gettime\0";
        let ptr = unsafe { libc::dlsym(libc::RTLD_NEXT, symbol.as_ptr().cast()) };
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { std::mem::transmute::<*mut libc::c_void, ClockGettimeFn>(ptr) })
        }
    })
}

fn simulated_timespec(clockid: libc::clockid_t) -> Option<libc::timespec> {
    if !TickContext::in_node_context() {
        return None;
    }
    let elapsed_ns = sim_elapsed_ns();
    let realtime_ns = sim_wall_epoch_ns().saturating_add(elapsed_ns);
    match clockid {
        libc::CLOCK_MONOTONIC | libc::CLOCK_MONOTONIC_RAW => Some(ns_to_timespec(elapsed_ns)),
        libc::CLOCK_REALTIME => Some(ns_to_timespec(realtime_ns)),
        #[cfg(target_os = "linux")]
        libc::CLOCK_MONOTONIC_COARSE | libc::CLOCK_BOOTTIME => Some(ns_to_timespec(elapsed_ns)),
        #[cfg(target_os = "linux")]
        libc::CLOCK_REALTIME_COARSE => Some(ns_to_timespec(realtime_ns)),
        #[cfg(target_os = "macos")]
        libc::CLOCK_UPTIME_RAW => Some(ns_to_timespec(elapsed_ns)),
        _ => None,
    }
}

#[unsafe(no_mangle)]
#[inline(never)]
unsafe extern "C" fn clock_gettime(
    clockid: libc::clockid_t,
    tp: *mut libc::timespec,
) -> libc::c_int {
    if USE_SIM_CLOCKS.load(Ordering::Acquire) > 0
        && let Some(timespec) = simulated_timespec(clockid)
    {
        unsafe { tp.write(timespec) };
        return 0;
    }

    if let Some(real) = real_clock_gettime() {
        let rc = unsafe { real(clockid, tp) };
        if rc == 0 {
            return 0;
        }
        return rc;
    }

    unsafe {
        tp.write(libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        });
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_guard_refcount_nesting() {
        let _lock = clock_test_lock();
        assert_eq!(USE_SIM_CLOCKS.load(Ordering::Acquire), 0);
        let outer = ClockGuard::init();
        assert_eq!(USE_SIM_CLOCKS.load(Ordering::Acquire), 1);
        {
            let _inner = ClockGuard::init();
            assert_eq!(USE_SIM_CLOCKS.load(Ordering::Acquire), 2);
        }
        assert_eq!(USE_SIM_CLOCKS.load(Ordering::Acquire), 1);
        drop(outer);
        assert_eq!(USE_SIM_CLOCKS.load(Ordering::Acquire), 0);
        assert_eq!(ACTIVE_SIM_THREAD.load(Ordering::Acquire), 0);
    }

    #[test]
    fn reset_zeroes_elapsed_and_sets_epoch() {
        let _lock = clock_test_lock();
        publish_sim_elapsed(Duration::from_millis(999));
        reset_sim_clock_state(Duration::from_secs(1_700_000_000));
        assert_eq!(sim_elapsed_ns(), 0);
        assert_eq!(sim_wall_epoch_ns(), 1_700_000_000 * 1_000_000_000);
    }
}

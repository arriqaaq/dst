
#![allow(unsafe_code)]

#[cfg(all(unix, feature = "os-clock-hooks"))]
mod clock;
#[cfg(all(unix, feature = "os-rng-hooks"))]
mod rand;

#[cfg(all(unix, feature = "os-clock-hooks"))]
pub use clock::{
    ClockGuard, clock_test_lock, publish_sim_elapsed, reset_sim_clock_state, set_sim_wall_epoch,
};
#[cfg(all(unix, feature = "os-rng-hooks"))]
pub use rand::{set_os_rng, set_os_rng_from_seed};

#[cfg(all(unix, feature = "os-clock-hooks"))]
pub fn sim_elapsed() -> Option<std::time::Duration> {
    let ns = clock::sim_elapsed_ns();
    if ns == 0 {
        None
    } else {
        Some(std::time::Duration::from_nanos(ns))
    }
}

#[cfg(all(unix, feature = "os-clock-hooks", feature = "os-rng-hooks"))]
impl ClockGuard {
    pub fn install(seed: u64) -> Self {
        set_os_rng_from_seed(seed);
        Self::init()
    }
}


#[cfg(not(all(unix, feature = "os-clock-hooks")))]
pub fn publish_sim_elapsed(_elapsed: std::time::Duration) {}

#[cfg(not(all(unix, feature = "os-clock-hooks")))]
pub fn set_sim_wall_epoch(_epoch: std::time::Duration) {}

#[cfg(not(all(unix, feature = "os-clock-hooks")))]
pub fn reset_sim_clock_state(_epoch: std::time::Duration) {}

#[cfg(not(all(unix, feature = "os-clock-hooks")))]
pub fn clock_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static STUB_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    STUB_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(not(all(unix, feature = "os-clock-hooks")))]
pub fn sim_elapsed() -> Option<std::time::Duration> {
    None
}

#[cfg(not(all(unix, feature = "os-clock-hooks")))]
#[derive(Debug)]
pub struct ClockGuard;

#[cfg(not(all(unix, feature = "os-clock-hooks")))]
impl ClockGuard {
    pub fn init() -> Self {
        Self
    }
}

#[cfg(any(
    not(all(unix, feature = "os-clock-hooks")),
    not(feature = "os-rng-hooks")
))]
impl ClockGuard {
    pub fn install(_seed: u64) -> Self {
        #[cfg(feature = "os-rng-hooks")]
        set_os_rng_from_seed(_seed);
        Self::init()
    }
}

#[cfg(not(all(unix, feature = "os-rng-hooks")))]
pub fn set_os_rng_from_seed(_seed: u64) {}

#[cfg(not(all(unix, feature = "os-rng-hooks")))]
pub fn set_os_rng(_rng: rand::rngs::StdRng) {
    let _ = _rng;
}

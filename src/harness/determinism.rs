use crate::sim::history::RunSummary;

pub fn check_determinism_enabled() -> bool {
    std::env::var("DST_CHECK_DETERMINISM").is_ok()
}

pub fn verify_same_seed_twice<F: FnMut() -> RunSummary>(run: F) {
    verify_same_seed_n(2, run);
}

pub fn verify_same_seed_n<F: FnMut() -> RunSummary>(n: usize, mut run: F) {
    assert!(n >= 2, "verify_same_seed_n needs n >= 2, got {n}");
    let first = run();
    for _ in 1..n {
        let next = run();
        assert_same_seed_twice(&first, &next);
    }
}

pub fn assert_same_seed_twice(a: &RunSummary, b: &RunSummary) {
    assert_eq!(
        a.steps, b.steps,
        "non-determinism detected: steps differ ({} vs {})",
        a.steps, b.steps
    );
    assert_eq!(
        a.history_hash, b.history_hash,
        "non-determinism detected: history hash differs"
    );
    assert_eq!(
        a.final_elapsed, b.final_elapsed,
        "non-determinism detected: elapsed differs ({:?} vs {:?})",
        a.final_elapsed, b.final_elapsed
    );
}

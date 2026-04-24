use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Scenario {
    pub seed: u64,
    pub max_simulated_time: Duration,
    pub label: String,
    pub fault_digest: String,
    pub repro_test_filter: Option<String>,
}

impl Scenario {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            max_simulated_time: Duration::from_secs(10),
            label: String::new(),
            fault_digest: String::new(),
            repro_test_filter: None,
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }

    pub fn with_test_filter(mut self, filter: impl Into<String>) -> Self {
        self.repro_test_filter = Some(filter.into());
        self
    }
}

pub fn resolve_seed(default: u64) -> u64 {
    std::env::var("DST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

pub fn repro_command_line(scenario: &Scenario) -> String {
    let mut cmd = format!("DST_SEED={}", scenario.seed);
    if let Some(ref filter) = scenario.repro_test_filter {
        cmd.push_str(&format!(" cargo test {filter} -- --exact"));
    } else {
        cmd.push_str(" cargo test");
    }
    cmd
}

use std::time::Duration;

use super::core::Sim;

#[derive(Debug, Clone)]
pub struct LinkConfig {
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub loss_probability: f64,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            min_latency: Duration::from_millis(0),
            max_latency: Duration::from_millis(100),
            loss_probability: 0.0,
        }
    }
}

pub const DEFAULT_WALL_CLOCK_EPOCH: Duration = Duration::from_secs(1_700_000_000);

#[derive(Debug, Clone)]
pub struct Config {
    pub tick: Duration,
    pub max_duration: Duration,
    pub rng_seed: u64,
    pub max_inflight: usize,
    pub udp_capacity: usize,
    pub link: LinkConfig,
    pub random_node_order: bool,
    pub epoch: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(1),
            max_duration: Duration::from_secs(10),
            rng_seed: 0,
            max_inflight: 10_000,
            udp_capacity: 64,
            link: LinkConfig::default(),
            random_node_order: false,
            epoch: DEFAULT_WALL_CLOCK_EPOCH,
        }
    }
}

pub struct Builder {
    config: Config,
}

impl Builder {
    pub fn new() -> Self {
        Self {
            config: Config::default(),
        }
    }

    pub fn simulation_duration(mut self, d: Duration) -> Self {
        self.config.max_duration = d;
        self
    }

    pub fn tick_duration(mut self, d: Duration) -> Self {
        self.config.tick = d;
        self
    }

    pub fn rng_seed(mut self, seed: u64) -> Self {
        self.config.rng_seed = seed;
        self
    }

    pub fn max_inflight(mut self, n: usize) -> Self {
        self.config.max_inflight = n;
        self
    }

    pub fn udp_capacity(mut self, n: usize) -> Self {
        self.config.udp_capacity = n;
        self
    }

    pub fn min_message_latency(mut self, d: Duration) -> Self {
        self.config.link.min_latency = d;
        self
    }

    pub fn max_message_latency(mut self, d: Duration) -> Self {
        self.config.link.max_latency = d;
        self
    }

    pub fn message_loss_rate(mut self, rate: f64) -> Self {
        self.config.link.loss_probability = rate;
        self
    }

    pub fn link_config(mut self, config: LinkConfig) -> Self {
        self.config.link = config;
        self
    }

    pub fn random_node_order(mut self, enabled: bool) -> Self {
        self.config.random_node_order = enabled;
        self
    }

    pub fn wall_clock_epoch(mut self, epoch: Duration) -> Self {
        self.config.epoch = epoch;
        self
    }

    pub fn build(self) -> Sim {
        Sim::new(self.config)
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

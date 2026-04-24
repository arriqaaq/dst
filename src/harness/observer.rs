use std::time::Duration;

use crate::error::Error;

#[derive(Debug, Clone)]
pub struct StepStats {
    pub steps: u64,
    pub elapsed: Duration,
    pub events_since_last_observer: u64,
    pub packets_delivered: u64,
    pub packets_dropped: u64,
    pub faults_applied: u64,
}

pub trait Observer {
    fn on_step_end(&mut self, digest: &StepStats) -> Result<(), Error>;
}

#[derive(Debug, Default)]
pub struct NoopObserver;

impl Observer for NoopObserver {
    fn on_step_end(&mut self, _digest: &StepStats) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ProgressWatchdog {
    max_idle_steps: u64,
    idle_counter: u64,
}

impl ProgressWatchdog {
    pub fn new(max_idle_steps: u64) -> Self {
        Self {
            max_idle_steps,
            idle_counter: 0,
        }
    }
}

impl Observer for ProgressWatchdog {
    fn on_step_end(&mut self, digest: &StepStats) -> Result<(), Error> {
        if digest.events_since_last_observer == 0 {
            self.idle_counter += 1;
            if self.idle_counter >= self.max_idle_steps {
                return Err(Error::NoProgress {
                    steps: self.idle_counter,
                    limit: self.max_idle_steps,
                });
            }
        } else {
            self.idle_counter = 0;
        }
        Ok(())
    }
}

use crate::error::Error;
use crate::ids::NodeName;
use crate::prng::Prng;
use crate::sim::core::Sim;

use rand::seq::SliceRandom;

#[derive(Debug)]
pub struct RollingRestart {
    pub nodes: Vec<NodeName>,
    pub ticks_between_crash: u64,
    pub ticks_down: u64,
    state: RollingRestartState,
}

#[derive(Debug)]
enum RollingRestartState {
    NotStarted,
    Running {
        order: Vec<NodeName>,
        index: usize,
        phase: RollingRestartPhase,
        next_action_tick: u64,
    },
    Done,
}

#[derive(Debug)]
enum RollingRestartPhase {
    WaitingToCrash,
    Down,
}

impl RollingRestart {
    pub fn new(nodes: Vec<NodeName>, ticks_between_crash: u64, ticks_down: u64) -> Self {
        Self {
            nodes,
            ticks_between_crash,
            ticks_down,
            state: RollingRestartState::NotStarted,
        }
    }

    pub fn tick(&mut self, sim: &mut Sim, current_step: u64, rng: &mut Prng) -> Result<(), Error> {
        match &mut self.state {
            RollingRestartState::NotStarted => {
                let mut order = self.nodes.clone();
                order.shuffle(rng.inner_mut());
                self.state = RollingRestartState::Running {
                    order,
                    index: 0,
                    phase: RollingRestartPhase::WaitingToCrash,
                    next_action_tick: current_step,
                };
                self.tick(sim, current_step, rng)
            }
            RollingRestartState::Running {
                order,
                index,
                phase,
                next_action_tick,
            } => {
                if current_step < *next_action_tick {
                    return Ok(());
                }

                if *index >= order.len() {
                    self.state = RollingRestartState::Done;
                    return Ok(());
                }

                match phase {
                    RollingRestartPhase::WaitingToCrash => {
                        let node = &order[*index];
                        sim.crash(node.clone());
                        *phase = RollingRestartPhase::Down;
                        *next_action_tick = current_step + self.ticks_down;
                    }
                    RollingRestartPhase::Down => {
                        let node = &order[*index];
                        sim.bounce(node.clone())?;
                        *index += 1;
                        *phase = RollingRestartPhase::WaitingToCrash;
                        *next_action_tick = current_step + self.ticks_between_crash;
                    }
                }
                Ok(())
            }
            RollingRestartState::Done => Ok(()),
        }
    }

    pub fn is_done(&self) -> bool {
        matches!(self.state, RollingRestartState::Done)
    }
}

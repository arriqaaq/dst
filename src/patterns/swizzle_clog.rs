use crate::ids::NodeName;
use crate::prng::Prng;
use crate::sim::core::Sim;

use rand::seq::SliceRandom;

#[derive(Debug)]
pub struct RollingNetworkClog {
    pub subset_size: usize,
    pub ticks_between: u64,
    pub nodes: Vec<NodeName>,
    state: RollingNetworkClogState,
}

#[derive(Debug)]
enum RollingNetworkClogState {
    NotStarted,
    Clogging {
        all_pairs: Vec<(NodeName, NodeName)>,
        clogged: Vec<(NodeName, NodeName)>,
        next_clog_tick: u64,
        clog_index: usize,
    },
    Unclogging {
        unclog_order: Vec<(NodeName, NodeName)>,
        next_unclog_tick: u64,
        unclog_index: usize,
    },
    Done,
}

impl RollingNetworkClog {
    pub fn new(nodes: Vec<NodeName>, subset_size: usize, ticks_between: u64) -> Self {
        Self {
            subset_size: subset_size.min(nodes.len()),
            ticks_between,
            nodes,
            state: RollingNetworkClogState::NotStarted,
        }
    }

    pub fn tick(&mut self, sim: &mut Sim, current_step: u64, rng: &mut Prng) {
        match &mut self.state {
            RollingNetworkClogState::NotStarted => {
                let mut candidates = self.nodes.clone();
                candidates.shuffle(rng.inner_mut());
                let subset: Vec<NodeName> = candidates.into_iter().take(self.subset_size).collect();

                let mut all_pairs = Vec::new();
                for s in &subset {
                    for n in &self.nodes {
                        if s != n {
                            all_pairs.push((s.clone(), n.clone()));
                        }
                    }
                }

                self.state = RollingNetworkClogState::Clogging {
                    all_pairs,
                    clogged: Vec::new(),
                    next_clog_tick: current_step,
                    clog_index: 0,
                };

                self.tick(sim, current_step, rng);
            }
            RollingNetworkClogState::Clogging {
                all_pairs,
                clogged,
                next_clog_tick,
                clog_index,
            } => {
                if current_step < *next_clog_tick {
                    return;
                }

                if *clog_index < all_pairs.len() {
                    let (a, b) = all_pairs[*clog_index].clone();
                    sim.hold(a.clone(), b.clone());
                    clogged.push((a, b));
                    *clog_index += 1;
                    *next_clog_tick = current_step + self.ticks_between;
                } else {
                    let mut unclog_order = clogged.clone();
                    unclog_order.shuffle(rng.inner_mut());
                    self.state = RollingNetworkClogState::Unclogging {
                        unclog_order,
                        next_unclog_tick: current_step + self.ticks_between,
                        unclog_index: 0,
                    };
                }
            }
            RollingNetworkClogState::Unclogging {
                unclog_order,
                next_unclog_tick,
                unclog_index,
            } => {
                if current_step < *next_unclog_tick {
                    return;
                }

                if *unclog_index < unclog_order.len() {
                    let (a, b) = unclog_order[*unclog_index].clone();
                    sim.release(a, b);
                    *unclog_index += 1;
                    *next_unclog_tick = current_step + self.ticks_between;
                } else {
                    self.state = RollingNetworkClogState::Done;
                }
            }
            RollingNetworkClogState::Done => {}
        }
    }

    pub fn is_done(&self) -> bool {
        matches!(self.state, RollingNetworkClogState::Done)
    }
}

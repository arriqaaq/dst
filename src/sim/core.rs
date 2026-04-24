use std::cell::RefCell;
use std::time::Duration;

use indexmap::IndexMap;

use crate::error::Error;
use crate::harness::observer::{Observer, StepStats};
use crate::ids::{NodeAddr, NodeName};
use crate::prng::Prng;
use crate::runtime::NodeRuntime;

use super::backplane::Direction;
use super::builder::Config;
use super::context::TickContext;
use super::filter::PacketFilter;
use super::history::Fault;
use super::history::{History, HistoryEvent, RunSummary};
use super::tick;

pub struct Sim {
    pub(crate) ctx: RefCell<TickContext>,
    pub(crate) runtimes: IndexMap<NodeAddr, NodeRuntime>,
    pub(crate) history: History,
    pub(crate) observers: Vec<Box<dyn Observer>>,
    pub(crate) steps: u64,
    pub(crate) max_duration: Duration,
    pub(crate) sim_tick: Duration,
    interim_events: u64,
    interim_faults: u64,
    interim_packet_drops: u64,
}

impl Sim {
    pub(crate) fn new(config: Config) -> Self {
        let sim_tick = config.tick;
        let max_duration = config.max_duration;
        let _epoch = config.epoch;
        #[cfg(all(feature = "os-clock-hooks", unix))]
        crate::os_hooks::reset_sim_clock_state(_epoch);
        let network = crate::sim::backplane::Network::new(config);
        Self {
            ctx: RefCell::new(TickContext::new(network)),
            runtimes: IndexMap::new(),
            history: History::new(10_000),
            observers: Vec::new(),
            steps: 0,
            max_duration,
            sim_tick,
            interim_events: 0,
            interim_faults: 0,
            interim_packet_drops: 0,
        }
    }

    fn account_init_align(&mut self) {
        self.ctx.borrow_mut().elapsed += NodeRuntime::INIT_ALIGN;
        #[cfg(all(feature = "os-clock-hooks", unix))]
        crate::os_hooks::publish_sim_elapsed(self.ctx.borrow().elapsed);
    }

    pub fn add_observer(&mut self, observer: Box<dyn Observer>) {
        self.observers.push(observer);
    }

    pub fn host<F, Fut>(&mut self, name: impl Into<NodeName>, task_factory: F) -> Result<(), Error>
    where
        F: Fn() -> Fut + 'static,
        Fut: std::future::Future<Output = crate::error::NodeResult> + 'static,
    {
        let name = name.into();
        if self.ctx.borrow().network.addrs.contains_key(&name) {
            return Err(Error::DuplicateNode {
                name: name.0.clone(),
            });
        }

        let sim_seed = self.ctx.borrow().network.config.rng_seed;
        let addr = self.ctx.borrow_mut().network.register_node(&name);
        let node_name = name.0.clone();
        self.ctx.borrow_mut().active_node = Some(addr);
        let rt = TickContext::activate(&self.ctx, || {
            NodeRuntime::new_host(node_name, sim_seed, move || Box::pin(task_factory()))
        })?;
        self.ctx.borrow_mut().active_node = None;
        self.runtimes.insert(addr, rt);
        self.account_init_align();
        Ok(())
    }

    pub fn client<Fut>(&mut self, name: impl Into<NodeName>, fut: Fut) -> Result<(), Error>
    where
        Fut: std::future::Future<Output = crate::error::NodeResult> + 'static,
    {
        let name = name.into();
        if self.ctx.borrow().network.addrs.contains_key(&name) {
            return Err(Error::DuplicateNode {
                name: name.0.clone(),
            });
        }

        let sim_seed = self.ctx.borrow().network.config.rng_seed;
        let addr = self.ctx.borrow_mut().network.register_node(&name);
        let node_name = name.0.clone();
        self.ctx.borrow_mut().active_node = Some(addr);
        let rt = TickContext::activate(&self.ctx, || {
            NodeRuntime::new_client(node_name, sim_seed, Box::pin(fut))
        })?;
        self.ctx.borrow_mut().active_node = None;
        self.runtimes.insert(addr, rt);
        self.account_init_align();
        Ok(())
    }

    pub fn step(&mut self) -> Result<bool, Error> {
        let output = tick::tick_step(tick::TickInput {
            ctx: &self.ctx,
            runtimes: &mut self.runtimes,
            steps: &mut self.steps,
            sim_tick: self.sim_tick,
            max_duration: self.max_duration,
        })?;

        let mut packets_delivered = 0u64;
        let mut packets_dropped = 0u64;
        let mut faults_applied = 0u64;
        for e in &output.events {
            match e {
                HistoryEvent::PacketDelivered { .. } => packets_delivered += 1,
                HistoryEvent::PacketDropped { .. }
                | HistoryEvent::PacketDroppedInboxFull { .. } => packets_dropped += 1,
                HistoryEvent::Fault(_) => faults_applied += 1,
                _ => {}
            }
        }
        let tick_events = output.events.len() as u64;
        for e in output.events {
            self.history.record(e);
        }

        let interim_events = self.interim_events;
        let interim_faults = self.interim_faults;
        let interim_packet_drops = self.interim_packet_drops;
        self.interim_events = 0;
        self.interim_faults = 0;
        self.interim_packet_drops = 0;

        if !self.observers.is_empty() {
            let summary = StepStats {
                steps: self.steps,
                elapsed: self.ctx.borrow().elapsed,
                events_since_last_observer: tick_events + interim_events,
                packets_delivered,
                packets_dropped: packets_dropped + interim_packet_drops,
                faults_applied: faults_applied + interim_faults,
            };
            for obs in &mut self.observers {
                obs.on_step_end(&summary)?;
            }
        }
        Ok(output.all_clients_done)
    }

    pub fn run(&mut self) -> Result<(), Error> {
        loop {
            if self.step()? {
                return Ok(());
            }
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.ctx.borrow().elapsed
    }

    pub fn steps(&self) -> u64 {
        self.steps
    }

    pub fn seed(&self) -> u64 {
        self.ctx.borrow().network.config.rng_seed
    }

    pub fn derive_rng(&self, salt: &[u8]) -> Prng {
        Prng::derive_stream(self.seed(), salt)
    }

    pub fn history(&self) -> &History {
        &self.history
    }

    pub fn run_summary(&self, clients_ok: bool) -> RunSummary {
        RunSummary {
            steps: self.steps,
            final_elapsed: self.ctx.borrow().elapsed,
            history_hash: self.history.digest(),
            clients_ok,
            total_events: self.history.total_count(),
        }
    }

    pub fn crash(&mut self, node: impl Into<NodeName>) {
        let name = node.into();
        let addr = self.ctx.borrow().network.addr_of(&name);
        if let Some(addr) = addr {
            if let Some(rt) = self.runtimes.get_mut(&addr) {
                let _ = rt.crash();
            }
            self.record_interim_fault(Fault::Crash { node: name.clone() });
            let dropped = {
                let mut ctx = self.ctx.borrow_mut();
                ctx.network.topology.crashed.insert(name.clone());
                ctx.network.drop_scheduled_for_node(&name)
            };
            self.record_packet_drops(dropped);
        }
    }

    pub fn bounce(&mut self, node: impl Into<NodeName>) -> Result<(), Error> {
        let name = node.into();
        let addr = self.ctx.borrow().network.addr_of(&name);
        if let Some(addr) = addr {
            self.ctx.borrow_mut().network.topology.crashed.remove(&name);
            if let Some(rt) = self.runtimes.get_mut(&addr) {
                self.ctx.borrow_mut().active_node = Some(addr);
                let result = TickContext::activate(&self.ctx, || rt.bounce());
                self.ctx.borrow_mut().active_node = None;
                result?;
                self.account_init_align();
            }
            self.record_interim_fault(Fault::Bounce { node: name });
            Ok(())
        } else {
            Err(Error::UnknownNode { name: name.0 })
        }
    }

    pub fn partition(&mut self, a: impl Into<NodeName>, b: impl Into<NodeName>) {
        let a = a.into();
        let b = b.into();
        self.record_interim_fault(Fault::PartitionUndirected {
            a: a.clone(),
            b: b.clone(),
        });
        let dropped = {
            let mut ctx = self.ctx.borrow_mut();
            ctx.network.topology.links.partition(&a, &b);
            ctx.network.drop_scheduled_between(&a, &b, Direction::Both)
        };
        self.record_packet_drops(dropped);
    }

    pub fn repair(&mut self, a: impl Into<NodeName>, b: impl Into<NodeName>) {
        let a = a.into();
        let b = b.into();
        self.ctx.borrow_mut().network.topology.links.repair(&a, &b);
        self.record_interim_fault(Fault::RepairUndirected {
            a: a.clone(),
            b: b.clone(),
        });
    }

    pub fn hold(&mut self, a: impl Into<NodeName>, b: impl Into<NodeName>) {
        let a = a.into();
        let b = b.into();
        self.record_interim_fault(Fault::Hold {
            a: a.clone(),
            b: b.clone(),
        });
        let now = self.ctx.borrow().elapsed;
        self.ctx.borrow_mut().network.topology.links.hold(&a, &b);
        self.ctx
            .borrow_mut()
            .network
            .hold_scheduled_between(&a, &b, now);
    }

    pub fn release(&mut self, a: impl Into<NodeName>, b: impl Into<NodeName>) {
        let a = a.into();
        let b = b.into();
        let now = self.ctx.borrow().elapsed;
        let released = self.ctx.borrow_mut().network.topology.links.release(&a, &b);
        self.record_interim_fault(Fault::Release {
            a: a.clone(),
            b: b.clone(),
        });
        self.ctx
            .borrow_mut()
            .network
            .reinsert_held_packets(released, now);
    }

    pub fn partition_oneway(&mut self, from: impl Into<NodeName>, to: impl Into<NodeName>) {
        let from = from.into();
        let to = to.into();
        self.record_interim_fault(Fault::PartitionOneway {
            from: from.clone(),
            to: to.clone(),
        });
        let dropped = {
            let mut ctx = self.ctx.borrow_mut();
            ctx.network.topology.partition_oneway(&from, &to);
            ctx.network
                .drop_scheduled_between(&from, &to, Direction::OneWay)
        };
        self.record_packet_drops(dropped);
    }

    pub fn repair_oneway(&mut self, from: impl Into<NodeName>, to: impl Into<NodeName>) {
        let from = from.into();
        let to = to.into();
        self.ctx
            .borrow_mut()
            .network
            .topology
            .repair_oneway(&from, &to);
        self.record_interim_fault(Fault::RepairOneway {
            from: from.clone(),
            to: to.clone(),
        });
    }

    pub fn add_packet_filter(&mut self, filter: Box<dyn PacketFilter>) {
        let name = filter.name().to_owned();
        self.ctx.borrow_mut().network.filters.add(filter);
        self.record_interim_fault(Fault::FilterAdded { name });
    }

    pub fn clear_filters(&mut self) {
        self.ctx.borrow_mut().network.filters.clear();
        self.record_interim_fault(Fault::FilterCleared);
    }

    pub fn network(&self) -> std::cell::Ref<'_, crate::sim::backplane::Network> {
        std::cell::Ref::map(self.ctx.borrow(), |c| &c.network)
    }

    pub fn network_mut(&mut self) -> std::cell::RefMut<'_, crate::sim::backplane::Network> {
        std::cell::RefMut::map(self.ctx.borrow_mut(), |c| &mut c.network)
    }

    fn record_packet_drops(&mut self, dropped: Vec<u64>) {
        let count = dropped.len() as u64;
        for seq in dropped {
            self.history.record(HistoryEvent::PacketDropped { seq });
        }
        self.interim_events = self.interim_events.saturating_add(count);
        self.interim_packet_drops = self.interim_packet_drops.saturating_add(count);
    }

    fn record_interim_fault(&mut self, fault: Fault) {
        self.history.record(HistoryEvent::Fault(fault));
        self.interim_events = self.interim_events.saturating_add(1);
        self.interim_faults = self.interim_faults.saturating_add(1);
    }
}

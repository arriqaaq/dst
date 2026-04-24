use std::cell::RefCell;
use std::time::Duration;

use indexmap::IndexMap;
use rand::seq::SliceRandom;

use crate::error::Error;
use crate::ids::NodeAddr;
use crate::runtime::NodeRuntime;

use super::context::TickContext;
use super::history::HistoryEvent;

pub(crate) struct TickInput<'a> {
    pub ctx: &'a RefCell<TickContext>,
    pub runtimes: &'a mut IndexMap<NodeAddr, NodeRuntime>,
    pub steps: &'a mut u64,
    pub sim_tick: Duration,
    pub max_duration: Duration,
}

pub(crate) struct TickOutput {
    pub all_clients_done: bool,
    pub events: Vec<HistoryEvent>,
}

pub(crate) fn tick_step(input: TickInput<'_>) -> Result<TickOutput, Error> {
    let TickInput {
        ctx,
        runtimes,
        steps,
        sim_tick,
        max_duration,
    } = input;

    let now = ctx.borrow().elapsed;

    ctx.borrow_mut().network.deliver_due_packets(now);

    let mut running: Vec<NodeAddr> = Vec::new();
    for (&addr, rt) in runtimes.iter() {
        if !rt.is_crashed() {
            running.push(addr);
        }
    }

    if ctx.borrow().network.config.random_node_order {
        running.shuffle(ctx.borrow_mut().network.rng.inner_mut());
    }

    let mut all_clients_done = true;

    for &addr in &running {
        ctx.borrow_mut().active_node = Some(addr);

        let rt = runtimes.get_mut(&addr).ok_or_else(|| Error::UnknownNode {
            name: format!("{addr:?}"),
        })?;

        let finished = TickContext::activate(ctx, || rt.tick(sim_tick))?;

        ctx.borrow_mut().active_node = None;

        if rt.is_client() && !finished {
            all_clients_done = false;
        }
    }

    ctx.borrow_mut().elapsed += sim_tick;
    *steps += 1;

    #[cfg(all(feature = "os-clock-hooks", unix))]
    crate::os_hooks::publish_sim_elapsed(ctx.borrow().elapsed);

    let elapsed = ctx.borrow().elapsed;
    if elapsed > max_duration && !all_clients_done {
        return Err(Error::DurationExceeded {
            elapsed,
            limit: max_duration,
        });
    }

    let has_clients = runtimes.values().any(|rt| rt.is_client());
    let finished = has_clients && all_clients_done;

    let events = std::mem::take(&mut ctx.borrow_mut().network.pending_events);

    Ok(TickOutput {
        all_clients_done: finished,
        events,
    })
}

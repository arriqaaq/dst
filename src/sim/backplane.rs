use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::net::SocketAddr;
use std::time::Duration;

use rand::Rng;
use tokio::sync::mpsc;

use crate::error::Error;
use crate::ids::{AddrPool, NodeAddr, NodeName};
use crate::net::udp::InboundPacket;
use crate::prng::Prng;
use crate::topology::{HeldPacket, Topology};

use super::builder::Config;
use super::filter::{FilterChain, FilterDecision};
use super::history::HistoryEvent;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScheduledPacket {
    pub deliver_at: Duration,
    pub seq: u64,
    pub from: SocketAddr,
    pub to: SocketAddr,
    pub payload: Vec<u8>,
}

impl Ord for ScheduledPacket {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deliver_at
            .cmp(&other.deliver_at)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for ScheduledPacket {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum Direction {
    Both,
    OneWay,
}

#[derive(Debug)]
pub struct Network {
    pub(crate) config: Config,
    pub(crate) rng: Prng,
    pub(crate) topology: Topology,
    pub(crate) filters: FilterChain,
    pub(crate) scheduled_packets: BinaryHeap<Reverse<ScheduledPacket>>,
    pub(crate) next_packet_seq: u64,
    pub(crate) addrs: BTreeMap<NodeName, NodeAddr>,
    pub(crate) names: BTreeMap<NodeAddr, NodeName>,
    pub(crate) addr_pool: AddrPool,
    pub(crate) inboxes: BTreeMap<SocketAddr, mpsc::Sender<InboundPacket>>,
    pub(crate) pending_events: Vec<HistoryEvent>,
}

impl Network {
    pub fn new(config: Config) -> Self {
        let rng = Prng::from_seed(config.rng_seed);
        Self {
            config,
            rng,
            topology: Topology::new(),
            filters: FilterChain::new(),
            scheduled_packets: BinaryHeap::new(),
            next_packet_seq: 0,
            addrs: BTreeMap::new(),
            names: BTreeMap::new(),
            addr_pool: AddrPool::new(),
            inboxes: BTreeMap::new(),
            pending_events: Vec::new(),
        }
    }

    pub fn register_node(&mut self, name: &NodeName) -> NodeAddr {
        if let Some(&addr) = self.addrs.get(name) {
            return addr;
        }
        let ip = self.addr_pool.allocate();
        let addr = NodeAddr::new(ip, 0);
        self.addrs.insert(name.clone(), addr);
        self.names.insert(addr, name.clone());
        addr
    }

    pub fn addr_of(&self, name: &NodeName) -> Option<NodeAddr> {
        self.addrs.get(name).copied()
    }

    pub fn name_of(&self, addr: &NodeAddr) -> Option<&NodeName> {
        self.names.get(addr)
    }

    pub fn register_socket(
        &mut self,
        addr: SocketAddr,
        tx: mpsc::Sender<InboundPacket>,
    ) -> Result<(), Error> {
        if let Some(existing) = self.inboxes.get(&addr)
            && !existing.is_closed()
        {
            return Err(Error::Io(format!("address already in use: {addr}")));
        }
        self.inboxes.insert(addr, tx);
        Ok(())
    }

    pub fn unregister_socket(&mut self, addr: &SocketAddr) {
        self.inboxes.remove(addr);
    }

    pub fn enqueue_packet(
        &mut self,
        from: SocketAddr,
        to: SocketAddr,
        payload: Vec<u8>,
        now: Duration,
    ) {
        let seq = self.next_packet_seq;
        self.next_packet_seq += 1;

        let from_name = self.name_of_ip(from.ip());
        let to_name = self.name_of_ip(to.ip());

        if let (Some(from_n), Some(to_n)) = (&from_name, &to_name) {
            if self.topology.crashed.contains(from_n) || self.topology.crashed.contains(to_n) {
                self.pending_events
                    .push(HistoryEvent::PacketDropped { seq });
                return;
            }

            if self
                .topology
                .oneway_blocks
                .contains_key(&(from_n.clone(), to_n.clone()))
            {
                self.pending_events
                    .push(HistoryEvent::PacketDropped { seq });
                return;
            }

            let link_held = self.topology.is_held(from_n, to_n);
            if !link_held && !self.topology.links.can_send_undirected(from_n, to_n) {
                self.pending_events
                    .push(HistoryEvent::PacketDropped { seq });
                return;
            }

            let link_config = self.topology.links.link_config(from_n, to_n);
            let loss_p = link_config
                .map(|c| c.loss_probability)
                .unwrap_or(self.config.link.loss_probability);
            if loss_p > 0.0 && self.rng.inner_mut().random_bool(loss_p) {
                self.pending_events
                    .push(HistoryEvent::PacketDropped { seq });
                return;
            }

            let extra_delay = if !self.filters.is_empty() {
                use super::filter::PacketMeta;
                let meta = PacketMeta {
                    from,
                    to,
                    from_name: from_name.as_ref(),
                    to_name: to_name.as_ref(),
                    payload: &payload,
                };
                match self.filters.evaluate(&meta) {
                    FilterDecision::Drop => {
                        self.pending_events
                            .push(HistoryEvent::PacketDropped { seq });
                        return;
                    }
                    FilterDecision::Delay(d) => d,
                    FilterDecision::Pass => Duration::ZERO,
                }
            } else {
                Duration::ZERO
            };

            let (min_lat, max_lat) = link_config
                .map(|c| (c.min_latency, c.max_latency))
                .unwrap_or((self.config.link.min_latency, self.config.link.max_latency));

            let latency = if max_lat <= min_lat {
                min_lat
            } else {
                let min_ns = min_lat.as_nanos() as u64;
                let max_ns = max_lat.as_nanos() as u64;
                let jitter_ns = self.rng.inner_mut().random_range(0u64..=(max_ns - min_ns));
                Duration::from_nanos(min_ns + jitter_ns)
            };

            let deliver_at = now + latency + extra_delay;
            let packet = ScheduledPacket {
                deliver_at,
                seq,
                from,
                to,
                payload,
            };

            if link_held {
                let inserted = self.topology.links.enqueue_held(
                    from_n,
                    to_n,
                    HeldPacket {
                        seq,
                        from,
                        to,
                        payload: packet.payload,
                        deliver_at,
                        held_at: now,
                    },
                );
                debug_assert!(inserted, "held link rejected held packet");
            } else if self.scheduled_packets.len() < self.config.max_inflight {
                self.scheduled_packets.push(Reverse(packet));
            } else {
                self.pending_events
                    .push(HistoryEvent::PacketDropped { seq });
            }
        }
    }

    pub(crate) fn drop_scheduled_between(
        &mut self,
        a: &NodeName,
        b: &NodeName,
        direction: Direction,
    ) -> Vec<u64> {
        self.take_scheduled_matching(|from, to| match direction {
            Direction::Both => (from == a && to == b) || (from == b && to == a),
            Direction::OneWay => from == a && to == b,
        })
        .into_iter()
        .map(|(packet, _, _)| packet.seq)
        .collect()
    }

    pub(crate) fn drop_scheduled_for_node(&mut self, node: &NodeName) -> Vec<u64> {
        self.take_scheduled_matching(|from, to| from == node || to == node)
            .into_iter()
            .map(|(packet, _, _)| packet.seq)
            .collect()
    }

    pub(crate) fn hold_scheduled_between(
        &mut self,
        a: &NodeName,
        b: &NodeName,
        now: Duration,
    ) -> usize {
        let held = self
            .take_scheduled_matching(|from, to| (from == a && to == b) || (from == b && to == a));
        let count = held.len();

        for (packet, from_name, to_name) in held {
            let inserted = self.topology.links.enqueue_held(
                &from_name,
                &to_name,
                HeldPacket {
                    seq: packet.seq,
                    from: packet.from,
                    to: packet.to,
                    payload: packet.payload,
                    deliver_at: packet.deliver_at,
                    held_at: now,
                },
            );
            debug_assert!(inserted, "held link rejected scheduled packet");
        }

        count
    }

    pub(crate) fn reinsert_held_packets(&mut self, packets: Vec<HeldPacket>, _now: Duration) {
        for packet in packets {
            self.scheduled_packets.push(Reverse(ScheduledPacket {
                deliver_at: packet.deliver_at,
                seq: packet.seq,
                from: packet.from,
                to: packet.to,
                payload: packet.payload,
            }));
        }
    }

    fn take_scheduled_matching<F>(
        &mut self,
        mut matches: F,
    ) -> Vec<(ScheduledPacket, NodeName, NodeName)>
    where
        F: FnMut(&NodeName, &NodeName) -> bool,
    {
        let drained: Vec<_> = self
            .scheduled_packets
            .drain()
            .map(|packet| packet.0)
            .collect();
        let mut affected = Vec::new();
        let mut unaffected = Vec::new();

        for packet in drained {
            if let Some((from_name, to_name)) = self.packet_names(&packet)
                && matches(&from_name, &to_name)
            {
                affected.push((packet, from_name, to_name));
            } else {
                unaffected.push(packet);
            }
        }

        self.scheduled_packets = unaffected.into_iter().map(Reverse).collect();
        affected.sort_by_key(|(packet, _, _)| (packet.deliver_at, packet.seq));
        affected
    }

    fn packet_names(&self, packet: &ScheduledPacket) -> Option<(NodeName, NodeName)> {
        let from = self.name_of_ip(packet.from.ip())?;
        let to = self.name_of_ip(packet.to.ip())?;
        Some((from, to))
    }

    pub fn deliver_due_packets(&mut self, now: Duration) {
        while let Some(Reverse(pkt)) = self.scheduled_packets.peek() {
            if pkt.deliver_at > now {
                break;
            }
            let pkt = self.scheduled_packets.pop().unwrap().0;
            if let Some(sender) = self.inboxes.get(&pkt.to) {
                let inbound = InboundPacket {
                    from: pkt.from,
                    payload: pkt.payload,
                };
                match sender.try_send(inbound) {
                    Ok(()) => {
                        self.pending_events
                            .push(HistoryEvent::PacketDelivered { seq: pkt.seq });
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.pending_events
                            .push(HistoryEvent::PacketDroppedInboxFull { seq: pkt.seq });
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        self.pending_events
                            .push(HistoryEvent::PacketDropped { seq: pkt.seq });
                    }
                }
            } else {
                self.pending_events
                    .push(HistoryEvent::PacketDropped { seq: pkt.seq });
            }
        }
    }

    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    fn name_of_ip(&self, ip: std::net::IpAddr) -> Option<NodeName> {
        for (addr, name) in &self.names {
            if addr.ip() == ip {
                return Some(name.clone());
            }
        }
        None
    }
}

use std::collections::VecDeque;
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::ids::NodeName;

#[derive(Debug, Clone)]
pub enum Fault {
    Crash { node: NodeName },
    Bounce { node: NodeName },
    PartitionUndirected { a: NodeName, b: NodeName },
    RepairUndirected { a: NodeName, b: NodeName },
    Hold { a: NodeName, b: NodeName },
    Release { a: NodeName, b: NodeName },
    PartitionOneway { from: NodeName, to: NodeName },
    RepairOneway { from: NodeName, to: NodeName },
    FilterAdded { name: String },
    FilterCleared,
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Crash { node } => write!(f, "crash({node})"),
            Self::Bounce { node } => write!(f, "bounce({node})"),
            Self::PartitionUndirected { a, b } => write!(f, "partition({a}, {b})"),
            Self::RepairUndirected { a, b } => write!(f, "repair({a}, {b})"),
            Self::Hold { a, b } => write!(f, "hold({a}, {b})"),
            Self::Release { a, b } => write!(f, "release({a}, {b})"),
            Self::PartitionOneway { from, to } => {
                write!(f, "partition_oneway({from} -> {to})")
            }
            Self::RepairOneway { from, to } => {
                write!(f, "repair_oneway({from} -> {to})")
            }
            Self::FilterAdded { name } => write!(f, "filter_added({name})"),
            Self::FilterCleared => write!(f, "filter_cleared"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum HistoryEvent {
    Fault(Fault),
    PacketDelivered { seq: u64 },
    PacketDropped { seq: u64 },
    PacketDroppedInboxFull { seq: u64 },
    Custom(String),
}

impl std::fmt::Display for HistoryEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fault(op) => write!(f, "fault: {op}"),
            Self::PacketDelivered { seq } => write!(f, "delivered: seq={seq}"),
            Self::PacketDropped { seq } => write!(f, "dropped: seq={seq}"),
            Self::PacketDroppedInboxFull { seq } => write!(f, "dropped_inbox_full: seq={seq}"),
            Self::Custom(msg) => write!(f, "custom: {msg}"),
        }
    }
}

#[derive(Debug)]
pub struct History {
    events: VecDeque<HistoryEvent>,
    capacity: usize,
    total_count: u64,
    hasher: Sha256,
}

impl History {
    pub fn new(capacity: usize) -> Self {
        Self {
            events: VecDeque::with_capacity(capacity),
            capacity,
            total_count: 0,
            hasher: Sha256::new(),
        }
    }

    pub fn record(&mut self, event: HistoryEvent) {
        let text = format!("{event}");
        self.hasher.update(text.as_bytes());
        self.hasher.update(self.total_count.to_le_bytes());

        self.total_count += 1;

        if self.events.len() >= self.capacity {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    pub fn events(&self) -> &VecDeque<HistoryEvent> {
        &self.events
    }

    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    pub fn snapshot(&self) -> Vec<String> {
        self.events.iter().map(|e| format!("{e}")).collect()
    }

    pub fn digest(&self) -> [u8; 32] {
        let h = self.hasher.clone();
        h.finalize().into()
    }
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub steps: u64,
    pub final_elapsed: Duration,
    pub history_hash: [u8; 32],
    pub clients_ok: bool,
    pub total_events: u64,
}

impl std::fmt::Display for RunSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RunSummary {{ steps: {}, elapsed: {:?}, hash: {}, events: {}, clients_ok: {} }}",
            self.steps,
            self.final_elapsed,
            hex(&self.history_hash[..8]),
            self.total_events,
            self.clients_ok,
        )
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

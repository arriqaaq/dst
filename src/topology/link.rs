use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;

use crate::ids::NodeName;
use crate::sim::builder::LinkConfig;

#[derive(Debug, Clone)]
pub struct HeldPacket {
    pub seq: u64,
    pub from: SocketAddr,
    pub to: SocketAddr,
    pub payload: Vec<u8>,
    pub deliver_at: Duration,
    pub held_at: Duration,
}

#[derive(Debug, Clone)]
pub enum LinkState {
    Healthy,
    Hold { pending: VecDeque<HeldPacket> },
    Partitioned,
}

impl LinkState {
    pub fn is_healthy(&self) -> bool {
        matches!(self, Self::Healthy)
    }

    pub fn is_partitioned(&self) -> bool {
        matches!(self, Self::Partitioned)
    }

    pub fn is_held(&self) -> bool {
        matches!(self, Self::Hold { .. })
    }
}

#[derive(Debug, Clone)]
pub struct Link {
    pub state: LinkState,
    pub config: Option<LinkConfig>,
}

impl Link {
    pub fn new() -> Self {
        Self {
            state: LinkState::Healthy,
            config: None,
        }
    }
}

impl Default for Link {
    fn default() -> Self {
        Self::new()
    }
}

fn canonical_pair(a: &NodeName, b: &NodeName) -> (NodeName, NodeName) {
    if a <= b {
        (a.clone(), b.clone())
    } else {
        (b.clone(), a.clone())
    }
}

#[derive(Debug, Default, Clone)]
pub struct Links {
    pairs: BTreeMap<(NodeName, NodeName), Link>,
}

impl Links {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_insert(&mut self, a: &NodeName, b: &NodeName) -> &mut Link {
        let key = canonical_pair(a, b);
        self.pairs.entry(key).or_default()
    }

    pub fn get(&self, a: &NodeName, b: &NodeName) -> Option<&Link> {
        let key = canonical_pair(a, b);
        self.pairs.get(&key)
    }

    pub fn get_mut(&mut self, a: &NodeName, b: &NodeName) -> Option<&mut Link> {
        let key = canonical_pair(a, b);
        self.pairs.get_mut(&key)
    }

    pub fn partition(&mut self, a: &NodeName, b: &NodeName) {
        let link = self.get_or_insert(a, b);
        link.state = LinkState::Partitioned;
    }

    pub fn repair(&mut self, a: &NodeName, b: &NodeName) {
        let link = self.get_or_insert(a, b);
        link.state = LinkState::Healthy;
    }

    pub fn hold(&mut self, a: &NodeName, b: &NodeName) {
        let link = self.get_or_insert(a, b);
        if !link.state.is_held() {
            link.state = LinkState::Hold {
                pending: VecDeque::new(),
            };
        }
    }

    pub fn release(&mut self, a: &NodeName, b: &NodeName) -> Vec<HeldPacket> {
        let link = self.get_or_insert(a, b);
        let mut released = Vec::new();
        if let LinkState::Hold { pending } = std::mem::replace(&mut link.state, LinkState::Healthy)
        {
            released.extend(pending);
        }
        released
    }

    pub fn enqueue_held(
        &mut self,
        from_name: &NodeName,
        to_name: &NodeName,
        packet: HeldPacket,
    ) -> bool {
        let (na, nb) = canonical_pair(from_name, to_name);
        if let Some(link) = self.pairs.get_mut(&(na.clone(), nb.clone()))
            && let LinkState::Hold { pending } = &mut link.state
        {
            pending.push_back(packet);
            return true;
        }
        false
    }

    pub fn can_send_undirected(&self, a: &NodeName, b: &NodeName) -> bool {
        match self.get(a, b) {
            None => true,
            Some(link) => link.state.is_healthy(),
        }
    }

    pub fn is_held(&self, a: &NodeName, b: &NodeName) -> bool {
        match self.get(a, b) {
            None => false,
            Some(link) => link.state.is_held(),
        }
    }

    pub fn link_config(&self, a: &NodeName, b: &NodeName) -> Option<&LinkConfig> {
        self.get(a, b).and_then(|l| l.config.as_ref())
    }

    pub fn set_link_config(&mut self, a: &NodeName, b: &NodeName, config: LinkConfig) {
        let link = self.get_or_insert(a, b);
        link.config = Some(config);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&(NodeName, NodeName), &Link)> {
        self.pairs.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(s: &str) -> NodeName {
        NodeName::new(s)
    }

    #[test]
    fn partition_and_repair() {
        let mut m = Links::new();
        let a = name("a");
        let b = name("b");

        assert!(m.can_send_undirected(&a, &b));
        m.partition(&a, &b);
        assert!(!m.can_send_undirected(&a, &b));
        m.repair(&a, &b);
        assert!(m.can_send_undirected(&a, &b));
    }

    #[test]
    fn hold_and_release() {
        let mut m = Links::new();
        let a = name("a");
        let b = name("b");

        m.hold(&a, &b);
        assert!(m.is_held(&a, &b));

        let packet = HeldPacket {
            seq: 0,
            from: "192.168.0.1:1000".parse().unwrap(),
            to: "192.168.0.2:1000".parse().unwrap(),
            payload: vec![1, 2, 3],
            deliver_at: Duration::from_millis(10),
            held_at: Duration::ZERO,
        };
        assert!(m.enqueue_held(&a, &b, packet));

        let released = m.release(&a, &b);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].seq, 0);
        assert!(m.can_send_undirected(&a, &b));
    }

    #[test]
    fn normalized_key_order() {
        let mut m = Links::new();
        let a = name("x");
        let b = name("y");
        m.partition(&b, &a);
        assert!(!m.can_send_undirected(&a, &b));
    }
}

pub mod link;

pub use link::{HeldPacket, Link, LinkState, Links};

use std::collections::{BTreeMap, BTreeSet};

use crate::ids::NodeName;

#[derive(Debug, Default, Clone)]
pub struct Topology {
    pub links: Links,
    pub oneway_blocks: BTreeMap<(NodeName, NodeName), ()>,
    pub crashed: BTreeSet<NodeName>,
}

impl Topology {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn partition_oneway(&mut self, from: &NodeName, to: &NodeName) {
        self.oneway_blocks.insert((from.clone(), to.clone()), ());
    }

    pub fn repair_oneway(&mut self, from: &NodeName, to: &NodeName) {
        self.oneway_blocks.remove(&(from.clone(), to.clone()));
    }

    fn oneway_open(&self, from: &NodeName, to: &NodeName) -> bool {
        !self.oneway_blocks.contains_key(&(from.clone(), to.clone()))
    }

    pub fn can_deliver(&self, from: &NodeName, to: &NodeName) -> bool {
        if self.crashed.contains(from) || self.crashed.contains(to) {
            return false;
        }
        if !self.oneway_open(from, to) {
            return false;
        }
        self.links.can_send_undirected(from, to)
    }

    pub fn is_held(&self, from: &NodeName, to: &NodeName) -> bool {
        self.links.is_held(from, to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(s: &str) -> NodeName {
        NodeName::new(s)
    }

    #[test]
    fn crashed_node_blocks_delivery() {
        let mut t = Topology::new();
        let a = name("a");
        let b = name("b");

        assert!(t.can_deliver(&a, &b));
        t.crashed.insert(b.clone());
        assert!(!t.can_deliver(&a, &b));
    }

    #[test]
    fn oneway_partition_in_topology() {
        let mut t = Topology::new();
        let a = name("a");
        let b = name("b");

        t.partition_oneway(&a, &b);
        assert!(!t.can_deliver(&a, &b));
        assert!(t.can_deliver(&b, &a));
        t.repair_oneway(&a, &b);
        assert!(t.can_deliver(&a, &b));
    }

    #[test]
    fn partition_in_topology() {
        let mut t = Topology::new();
        let a = name("a");
        let b = name("b");

        t.links.partition(&a, &b);
        assert!(!t.can_deliver(&a, &b));
        assert!(!t.can_deliver(&b, &a));
    }
}

use std::net::SocketAddr;
use std::time::Duration;

use crate::ids::NodeName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterDecision {
    Pass,
    Drop,
    Delay(Duration),
}

#[derive(Debug)]
pub struct PacketMeta<'a> {
    pub from: SocketAddr,
    pub to: SocketAddr,
    pub from_name: Option<&'a NodeName>,
    pub to_name: Option<&'a NodeName>,
    pub payload: &'a [u8],
}

pub trait PacketFilter {
    fn filter(&self, meta: &PacketMeta<'_>) -> FilterDecision;

    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }
}

#[derive(Default)]
pub struct FilterChain {
    filters: Vec<Box<dyn PacketFilter>>,
}

impl FilterChain {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, filter: Box<dyn PacketFilter>) {
        self.filters.push(filter);
    }

    pub fn clear(&mut self) {
        self.filters.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    pub fn evaluate(&self, meta: &PacketMeta<'_>) -> FilterDecision {
        for filter in &self.filters {
            let decision = filter.filter(meta);
            if decision != FilterDecision::Pass {
                return decision;
            }
        }
        FilterDecision::Pass
    }
}

impl std::fmt::Debug for FilterChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterChain")
            .field("count", &self.filters.len())
            .finish()
    }
}

pub struct ClosureFilter {
    name: String,
    func: Box<dyn Fn(&PacketMeta<'_>) -> FilterDecision>,
}

impl ClosureFilter {
    pub fn new(
        name: impl Into<String>,
        func: impl Fn(&PacketMeta<'_>) -> FilterDecision + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            func: Box::new(func),
        }
    }
}

impl PacketFilter for ClosureFilter {
    fn filter(&self, meta: &PacketMeta<'_>) -> FilterDecision {
        (self.func)(meta)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

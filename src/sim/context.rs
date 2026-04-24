use std::cell::RefCell;
use std::time::Duration;

use scoped_tls::scoped_thread_local;

use crate::ids::NodeAddr;
use crate::sim::backplane::Network;

scoped_thread_local!(static NODE_CTX: RefCell<TickContext>);

pub(crate) struct TickContext {
    pub(crate) network: Network,
    pub(crate) active_node: Option<NodeAddr>,
    pub(crate) elapsed: Duration,
}

impl TickContext {
    pub(crate) fn new(network: Network) -> Self {
        Self {
            network,
            active_node: None,
            elapsed: Duration::ZERO,
        }
    }

    pub(crate) fn with<R>(f: impl FnOnce(&mut TickContext) -> R) -> R {
        NODE_CTX.with(|cell| f(&mut cell.borrow_mut()))
    }

    #[cfg(all(feature = "os-clock-hooks", unix))]
    pub(crate) fn in_node_context() -> bool {
        NODE_CTX.is_set()
    }

    pub(crate) fn activate<R>(ctx: &RefCell<TickContext>, f: impl FnOnce() -> R) -> R {
        NODE_CTX.set(ctx, f)
    }
}

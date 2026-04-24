use std::pin::Pin;
use std::time::Duration;

use tokio::runtime::Runtime;
use tokio::task::{JoinHandle, LocalSet};

use crate::error::{Error, NodeResult};

pub type NodeTask = Pin<Box<dyn std::future::Future<Output = NodeResult> + 'static>>;

type NodeTaskFactory = Box<dyn Fn() -> NodeTask>;

pub struct NodeRuntime {
    tokio: Runtime,
    local: LocalSet,
    handle: Option<JoinHandle<NodeResult>>,
    task_factory: Option<NodeTaskFactory>,
    node_name: String,
    sim_seed: u64,
    is_client: bool,
    crashed: bool,
    finished: bool,
}

impl std::fmt::Debug for NodeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRuntime")
            .field("node_name", &self.node_name)
            .field("is_client", &self.is_client)
            .field("crashed", &self.crashed)
            .field("finished", &self.finished)
            .finish()
    }
}

impl NodeRuntime {
    pub(crate) const INIT_ALIGN: Duration = Duration::from_millis(1);

    fn build_runtime(sim_seed: u64, node_name: &str) -> Result<Runtime, Error> {
        let mut builder = tokio::runtime::Builder::new_current_thread();
        builder.enable_time().start_paused(true);

        #[cfg(all(feature = "tokio-rng-seed", tokio_unstable))]
        {
            use rand::RngCore;
            let node_seed =
                crate::prng::Prng::derive_stream(sim_seed, node_name.as_bytes()).next_u64();
            builder.rng_seed(tokio::runtime::RngSeed::from_bytes(
                &node_seed.to_le_bytes(),
            ));
        }
        let _ = (sim_seed, node_name);

        builder
            .build()
            .map_err(|e: std::io::Error| Error::Io(e.to_string()))
    }

    #[allow(clippy::async_yields_async)]
    fn spawn_and_init(tokio: &Runtime, local: &LocalSet, fut: NodeTask) -> JoinHandle<NodeResult> {
        tokio.block_on(local.run_until(async {
            let handle = tokio::task::spawn_local(fut);
            tokio::time::sleep(Self::INIT_ALIGN).await;
            handle
        }))
    }

    pub fn new_host(
        name: String,
        sim_seed: u64,
        factory: impl Fn() -> NodeTask + 'static,
    ) -> Result<Self, Error> {
        let tokio = Self::build_runtime(sim_seed, &name)?;
        let local = LocalSet::new();
        let factory: NodeTaskFactory = Box::new(factory);

        let handle = Self::spawn_and_init(&tokio, &local, (factory)());

        Ok(Self {
            tokio,
            local,
            handle: Some(handle),
            task_factory: Some(factory),
            node_name: name,
            sim_seed,
            is_client: false,
            crashed: false,
            finished: false,
        })
    }

    pub fn new_client(name: String, sim_seed: u64, fut: NodeTask) -> Result<Self, Error> {
        let tokio = Self::build_runtime(sim_seed, &name)?;
        let local = LocalSet::new();

        let handle = Self::spawn_and_init(&tokio, &local, fut);

        Ok(Self {
            tokio,
            local,
            handle: Some(handle),
            task_factory: None,
            node_name: name,
            sim_seed,
            is_client: true,
            crashed: false,
            finished: false,
        })
    }

    pub fn tick(&mut self, duration: Duration) -> Result<bool, Error> {
        if self.crashed || self.finished {
            return Ok(self.finished);
        }

        self.tokio.block_on(async {
            self.local
                .run_until(async {
                    tokio::time::sleep(duration).await;
                })
                .await;
        });

        if let Some(ref handle) = self.handle
            && handle.is_finished()
        {
            let Some(handle) = self.handle.take() else {
                return Ok(false);
            };
            let result = self.tokio.block_on(self.local.run_until(handle));
            self.finished = true;

            match result {
                Ok(Ok(())) => return Ok(true),
                Ok(Err(e)) => {
                    return Err(Error::NodeReturned {
                        node: self.node_name.clone(),
                        source: e,
                    });
                }
                Err(join_err) => {
                    if join_err.is_panic() {
                        return Err(Error::NodePanicked {
                            node: self.node_name.clone(),
                            reason: format!("{join_err}"),
                        });
                    }
                    return Err(Error::Join(join_err.to_string()));
                }
            }
        }

        Ok(false)
    }

    pub fn crash(&mut self) -> Result<(), Error> {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        self.crashed = true;
        self.finished = false;
        let rt = Self::build_runtime(self.sim_seed, &self.node_name)?;
        self.tokio = rt;
        self.local = LocalSet::new();
        Ok(())
    }

    pub fn bounce(&mut self) -> Result<(), Error> {
        self.crash()?;
        self.crashed = false;

        if let Some(ref factory) = self.task_factory {
            let handle = Self::spawn_and_init(&self.tokio, &self.local, (factory)());
            self.handle = Some(handle);
            Ok(())
        } else {
            Err(Error::Config("cannot bounce a client (no task factory)"))
        }
    }

    pub fn is_client(&self) -> bool {
        self.is_client
    }

    pub fn is_crashed(&self) -> bool {
        self.crashed
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }
}

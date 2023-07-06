mod executor_graph;
mod executor_task;
mod parallel;
mod physical;
mod runtime;

use std::{collections::VecDeque, sync::Arc};

use self::{
    executor_graph::ExecutorGraph, parallel::pipeline_builder::PipelineBuilder,
    physical::PhysicalOperatorRef, runtime::ExecutionRuntime,
};
use anyhow::Result;

#[derive(Clone)]
pub struct ExecutionContext {
    // pub context: ContextRef,
}

pub struct Executor {
    // pub context: ExecutionContext,
    pub graph: ExecutorGraph,
    pub threads_num: usize,
    pub runtime: Arc<ExecutionRuntime>,
}

impl Executor {
    pub fn initialize(plan: PhysicalOperatorRef) -> Result<Executor> {
        let mut builder = PipelineBuilder::new();
        let meta_pipeline = builder.finalize(plan)?;

        let graph = ExecutorGraph::from_pipeline(meta_pipeline)?;

        Ok(Executor {
            // context: ExecutionContext { context },
            graph,
            threads_num: 0,
            runtime: Arc::new(ExecutionRuntime::with_default_worker_threads()?),
        })
    }

    pub fn execute(self: &Arc<Self>) -> Result<()> {
        let mut schedule_queue = self.graph.init_schedule_queue(self.threads_num)?;
        Ok(())
    }

    pub fn complete_pipeline(&self) {}

    fn finish_task(&self) -> Result<()> {
        Ok(())
    }
}

use std::sync::Arc;

use super::pipeline::Pipeline;
use crate::executor::ExecutionContext;
use anyhow::Result;

pub enum PipelineExecuteResult {
    Finished,
    NotFinished,
    Interrupted,
}

/// The pipelineExcutor represents an execution pipeline.
pub struct PipelineExecutor {
    pub context: ExecutionContext,
    /// The pipeline to process.
    pub pipeline: Pipeline,
}

impl PipelineExecutor {
    pub fn create(mut pipeline: Pipeline) -> Result<Arc<PipelineExecutor>> {
        todo!()
    }

    /// Fully execute the pipeline with  a source and a sink util the source is
    /// completedly exhausted.
    pub fn execute(self: &Arc<Self>) -> Result<PipelineExecuteResult> {
        Self::execute_parital(&self, usize::MAX)
    }

    /// Execute a pipeline with a source and a sink until finished, or until
    /// max_chunks were processed from the source Returns true if execution
    /// is finished, false if Execute should be called again
    pub fn execute_parital(self: &Arc<Self>, max_chunks: usize) -> Result<PipelineExecuteResult> {
        todo!()
    }

    fn execute_push(self: &Arc<Self>) {}

    fn execute2(&self) -> Result<()> {
        todo!()
    }
}

impl Drop for PipelineExecutor {
    fn drop(&mut self) {
        todo!()
    }
}

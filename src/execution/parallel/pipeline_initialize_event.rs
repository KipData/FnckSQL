use std::sync::Arc;

use parking_lot::Mutex;

use crate::execution::executor_task::{Task, TaskExecutionMode, TaskExecutionResult};

use super::{
    pipeline::Pipeline,
    pipeline_event::{Event, PipelineEvent},
};

use anyhow::Result;

pub struct PipelineInitializeEvent;

impl Event for PipelineInitializeEvent {
    fn schedule(&self, pipeline: Arc<Pipeline>, event: Mutex<PipelineEvent>) -> Vec<Arc<dyn Task>> {
        vec![Arc::new(PipelineInitializeTask { pipeline, event })]
    }
}

pub struct PipelineInitializeTask {
    pipeline: Arc<Pipeline>,
    event: Mutex<PipelineEvent>,
}

#[async_trait::async_trait]
impl Task for PipelineInitializeTask {
    /// The name of task.
    fn name(&self) -> String {
        "PipelineInitializeTask".to_string()
    }

    async fn execute(&mut self, _mode: TaskExecutionMode) -> Result<TaskExecutionResult> {
        self.pipeline.reset_sink();
        let mut guard = self.event.lock();

        guard.finish_task();

        drop(guard);

        Ok(TaskExecutionResult::Finished)
    }
}

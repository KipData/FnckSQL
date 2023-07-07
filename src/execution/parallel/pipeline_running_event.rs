use std::sync::Arc;

use parking_lot::Mutex;

use super::{
    pipeline::Pipeline,
    pipeline_event::{Event, PipelineEvent},
};
use crate::execution::executor_task::Task;

pub struct PipelineRunningEvent;

impl Event for PipelineRunningEvent {
    fn schedule(&self, pipeline: Arc<Pipeline>, event: Mutex<PipelineEvent>) -> Vec<Arc<dyn Task>> {
        pipeline.schedule(event)
    }
}

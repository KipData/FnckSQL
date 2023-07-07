use std::sync::Arc;

use super::{pipeline::Pipeline, pipeline_event::Event};

pub struct PipelineFinishEvent(pub Arc<Pipeline>);

impl Event for PipelineFinishEvent {
    fn finish_event(&self) {
        self.0.finalize();
    }
}

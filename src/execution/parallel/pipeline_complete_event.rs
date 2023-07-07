use super::pipeline_event::Event;

pub struct PipelineCompleteEvent {
    // pub executor: Arc<Executor>,
    pub complete_pipeline: bool,
}

impl Event for PipelineCompleteEvent {
    fn finalize_finish(&self) {
        if self.complete_pipeline {
            // self.executor.complete_pipeline();
        }
    }
}

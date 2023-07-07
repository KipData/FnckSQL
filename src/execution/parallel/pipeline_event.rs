use std::sync::Arc;

use parking_lot::Mutex;
use petgraph::stable_graph::NodeIndex;

use super::{
    pipeline_complete_event::PipelineCompleteEvent, pipeline_finish_event::PipelineFinishEvent,
    pipeline_initialize_event::PipelineInitializeEvent,
    pipeline_running_event::PipelineRunningEvent,
};
use crate::execution::{
    executor::ExecutionQueue,
    executor_task::{ExecutionTask, Task},
    parallel::pipeline::Pipeline,
};

pub trait Event: Sync + Send + 'static {
    fn schedule(
        &self,
        _pipeline: Arc<Pipeline>,
        _event: Mutex<PipelineEvent>,
    ) -> Vec<Arc<dyn Task>> {
        vec![]
    }

    /// Called right after the event is finished.
    fn finish_event(&self) {}

    /// Called after the event is entirely finished.
    fn finalize_finish(&self) {}
}

pub struct PipelineEventStack {
    pub pipeline_initialize_event: NodeIndex,
    pub pipeline_event: NodeIndex,
    pub pipeline_finish_event: NodeIndex,
    pub pipeline_complete_event: NodeIndex,
}

#[derive(Clone)]
pub struct PipelineEvent {
    pub pipeline: Arc<Pipeline>,
    /// The current threads working on the event.
    pub finished_tasks: usize,

    /// The maximum amount of threads that can work on the event.
    pub total_tasks: usize,

    /// The amount of completed dependencies
    /// The event can only be started after the dependencies have finished
    /// executing.
    pub finished_dependencies: usize,

    /// The total amount of dependencies.
    pub total_dependencies: usize,

    /// The events that depend on this event to run
    pub parents: Vec<Box<PipelineEvent>>,

    /// Whether or not the event is finished executing.
    pub finished: bool,

    event: Arc<dyn Event>,
}

pub struct PipelineEventGuard(Mutex<PipelineEvent>);

impl PipelineEvent {
    pub fn create_initialize_event(
        // executor: Arc<Executor>,
        pipeline: Arc<Pipeline>,
    ) -> PipelineEvent {
        PipelineEvent::create(Arc::new(PipelineInitializeEvent), pipeline)
    }

    pub fn create_running_event(pipeline: Arc<Pipeline>) -> PipelineEvent {
        PipelineEvent::create(Arc::new(PipelineRunningEvent), pipeline)
    }

    pub fn create_finish_event(pipeline: Arc<Pipeline>) -> PipelineEvent {
        PipelineEvent::create(Arc::new(PipelineFinishEvent(pipeline.clone())), pipeline)
    }

    pub fn create_complete_event(
        complete_pipeline: bool,
        pipeline: Arc<Pipeline>,
    ) -> PipelineEvent {
        PipelineEvent::create(
            Arc::new(PipelineCompleteEvent {
                // executor: executor.clone(),
                complete_pipeline,
            }),
            pipeline,
        )
    }

    fn create(event: Arc<dyn Event>, pipeline: Arc<Pipeline>) -> Self {
        Self {
            pipeline,
            finished_tasks: 0,
            total_tasks: 0,
            finished_dependencies: 0,
            total_dependencies: 0,
            parents: vec![],
            finished: false,
            event,
        }
    }

    fn finish(&mut self) {
        assert!(self.finished);

        self.event.finish_event();
        self.finished = true;

        for entry in self.parents.iter_mut() {
            entry.complete_dependency();
        }
        self.event.finalize_finish();
    }

    pub fn complete_dependency(&mut self) {
        self.finished_dependencies += 1;
        let cur_finished = self.finished_dependencies;
        assert!(cur_finished <= self.total_dependencies);
        if cur_finished == self.total_dependencies {
            if self.total_tasks == 0 {
                self.event
                    .schedule(self.pipeline.clone(), Mutex::new(self.clone()));
                self.finish()
            }
        }
    }

    pub fn finish_task(&mut self) {
        let cur_tasks = self.total_tasks;
        self.finished_tasks += 1;
        let cur_finished = self.finished_tasks;
        assert!(cur_finished < cur_tasks);
        if cur_finished == cur_tasks {
            self.finish();
        }
    }

    fn has_dependencies(&self) -> bool {
        self.total_dependencies != 0
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn add_dependency(&mut self, event: Box<PipelineEvent>) {
        self.total_dependencies += 1;

        self.parents.push(event);
    }

    fn insert_event(&mut self, _replacement_event: Arc<PipelineEvent>) {
        // let mut parents = self.parents.replace(vec![]);

        // replacement_event.add_dependency(self);
        // parents.push(Arc::downgrade(&replacement_event));

        // self.executor.add_event(replacement_event); // Implement
        // // Executor::add_event() as per your requirements

        // self.parents.replace(parents);
    }

    pub async fn schedule(&mut self, global_execution_queue: &mut ExecutionQueue) {
        let inner_tasks = self
            .event
            .schedule(self.pipeline.clone(), Mutex::new(self.clone()));

        self.total_tasks = inner_tasks.len();
        inner_tasks.iter().for_each(|inner| {
            global_execution_queue.push_task(ExecutionTask::create(inner.clone()));
        });
    }

    pub fn finish_event(&mut self) {
        self.event.finish_event();
    }

    pub fn finalize_finish(&mut self) {
        self.event.finalize_finish();
    }
}

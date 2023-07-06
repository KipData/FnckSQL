use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use petgraph::stable_graph::NodeIndex;

use crate::executor::{executor_task::Task, parallel::pipeline::Pipeline};

pub trait Event {
    fn schedule(&self) -> Vec<Arc<dyn Task>> {
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

pub struct PipelineEvent {
    // pub executor: Arc<Executor>,
    pub pipeline: Arc<Pipeline>,
    /// The current threads working on the event.
    pub finished_tasks: AtomicUsize,

    /// The maximum amount of threads that can work on the event.
    pub total_tasks: AtomicUsize,

    /// The amount of completed dependencies
    /// The event can only be started after the dependencies have finished
    /// executing.
    pub finished_dependencies: AtomicUsize,

    /// The total amount of dependencies.
    pub total_dependencies: usize,

    /// The events that depend on this event to run
    pub parents: RefCell<Vec<Arc<PipelineEvent>>>,

    /// Whether or not the event is finished executing.
    pub finished: AtomicBool,

    event: Box<dyn Event>,
}

impl PipelineEvent {
    pub fn create_initialize_event(
        // executor: Arc<Executor>,
        pipeline: Arc<Pipeline>,
    ) -> PipelineEvent {
        PipelineEvent::create(Box::new(PipelineInitializeEvent), pipeline)
    }

    pub fn create_running_event(pipeline: Arc<Pipeline>) -> PipelineEvent {
        PipelineEvent::create(Box::new(PipelineRunningEvent(pipeline.clone())), pipeline)
    }

    pub fn create_finish_event(pipeline: Arc<Pipeline>) -> PipelineEvent {
        PipelineEvent::create(Box::new(PipelineFinishEvent(pipeline.clone())), pipeline)
    }

    pub fn create_complete_event(
        complete_pipeline: bool,
        pipeline: Arc<Pipeline>,
    ) -> PipelineEvent {
        PipelineEvent::create(
            Box::new(PipelineCompleteEvent {
                // executor: executor.clone(),
                complete_pipeline,
            }),
            pipeline,
        )
    }

    // fn create(event: E, executor: Arc<Executor>, pipeline: Arc<Pipeline>) -> Self
    // {     Self {
    //         executor,
    //         pipeline,
    //         finished_tasks: AtomicUsize::new(0),
    //         total_tasks: AtomicUsize::new(0),
    //         finished_dependencies: AtomicUsize::new(0),
    //         total_dependencies: 0,
    //         parents: RefCell::new(vec![]),
    //         finished: AtomicBool::new(false),
    //         event,
    //     }
    // }

    fn create(event: Box<dyn Event>, pipeline: Arc<Pipeline>) -> Self {
        Self {
            pipeline,
            finished_tasks: AtomicUsize::new(0),
            total_tasks: AtomicUsize::new(0),
            finished_dependencies: AtomicUsize::new(0),
            total_dependencies: 0,
            parents: RefCell::new(vec![]),
            finished: AtomicBool::new(false),
            event,
        }
    }

    pub fn complete_dependency(&self) {
        let cur_finished = self.finished_dependencies.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(cur_finished <= self.total_dependencies);
        if cur_finished == self.total_dependencies {
            if self.total_tasks.load(Ordering::SeqCst) == 0 {
                self.event.schedule();
                self.finish()
            }
        }
    }

    fn finish(&self) {
        assert!(self.finished.load(Ordering::SeqCst));

        self.event.finish_event();
        self.finished.store(true, Ordering::SeqCst);

        for entry in self.parents.borrow().iter() {
            entry.complete_dependency();
        }
        self.event.finalize_finish();
    }

    fn finish_task(&self) {
        let cur_tasks = self.total_tasks.load(Ordering::SeqCst);
        let cur_finished = self.finished_tasks.fetch_add(1, Ordering::SeqCst);
        assert!(cur_finished < cur_tasks);
        if cur_finished + 1 == cur_tasks {
            self.finish();
        }
    }

    fn has_dependencies(&self) -> bool {
        self.total_dependencies != 0
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    fn add_dependency(&mut self, event: Arc<PipelineEvent>) {
        self.total_dependencies += 1;

        self.parents.borrow_mut().push(event);
    }

    fn set_tasks(&mut self, tasks: Vec<Arc<dyn Task>>) {
        self.total_tasks.store(tasks.len(), Ordering::Relaxed);
        // schedule.
    }

    fn insert_event(&mut self, _replacement_event: Arc<PipelineEvent>) {
        // let mut parents = self.parents.replace(vec![]);

        // replacement_event.add_dependency(self);
        // parents.push(Arc::downgrade(&replacement_event));

        // self.executor.add_event(replacement_event); // Implement
        // // Executor::add_event() as per your requirements

        // self.parents.replace(parents);
    }

    pub fn schedule(&mut self) {
        let inner_tasks = self.event.schedule();
        self.set_tasks(inner_tasks);
    }

    pub fn finish_event(&mut self) {
        self.event.finish_event();
    }

    pub fn finalize_finish(&mut self) {
        self.event.finalize_finish();
    }
}

pub struct PipelineInitializeEvent;

impl Event for PipelineInitializeEvent {
    fn schedule(&self) -> Vec<Arc<dyn Task>> {
        vec![]
    }
}

pub struct PipelineInitializeTask {}

pub struct PipelineRunningEvent(pub Arc<Pipeline>);

impl Event for PipelineRunningEvent {
    fn schedule(&self) -> Vec<Arc<dyn Task>> {
        self.0.reset();
        if !self.0.can_parallel() {
            self.0.schedule_parallel()
        } else {
            self.0.schedule_sequential()
        }
    }
}

pub struct PipelineFinishEvent(pub Arc<Pipeline>);

impl Event for PipelineFinishEvent {
    fn finish_event(&self) {
        self.0.finalize();
    }
}

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

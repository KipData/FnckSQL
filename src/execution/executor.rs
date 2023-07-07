use std::{collections::VecDeque, sync::Arc};

use anyhow::Result;

use super::executor_graph::ExecutorGraph;
use super::executor_task::ExecutionTask;
use super::parallel::pipeline_builder::PipelineBuilder;
use super::physical::PhysicalOperatorRef;
use super::runtime::thread::Thread;
use super::runtime::thread::ThreadJoinHandle;
use super::runtime::ExecutionRuntime;
use super::runtime::TrySpawn;

pub struct ExecutionQueue(pub VecDeque<ExecutionTask>);

impl ExecutionQueue {
    pub fn push_task(&mut self, task: ExecutionTask) {
        self.0.push_back(task);
    }
}

pub struct ExecutionContext {
    pub global_execution_queue: ExecutionQueue,
}

impl ExecutionContext {
    pub fn get_global_execution_queue(&mut self) -> &mut ExecutionQueue {
        &mut self.global_execution_queue
    }
}

pub struct Executor {
    pub context: ExecutionContext,
    pub graph: ExecutorGraph,
    pub threads_num: usize,
    pub runtime: Arc<ExecutionRuntime>,
}

impl Executor {
    pub async fn initialize(plan: PhysicalOperatorRef) -> Result<Executor> {
        let mut builder = PipelineBuilder::new();
        let meta_pipeline = builder.finalize(plan)?;

        let graph = ExecutorGraph::from_pipeline(meta_pipeline)?;

        Ok(Executor {
            context: ExecutionContext {
                global_execution_queue: ExecutionQueue(VecDeque::new()),
            },
            graph,
            threads_num: 0,
            runtime: Arc::new(ExecutionRuntime::with_default_worker_threads()?),
        })
    }

    pub async fn execute(&mut self) -> Result<()> {
        // Schedule the pipelines that do not have dependencies.
        self.graph
            .init_schedule_event_queue(&mut self.context.global_execution_queue)
            .await;

        // Pull execution task from `GlobalExecutionQueue`.
        self.runtime.spawn(ExecutionTask::empty());
        Ok(())
    }

    fn execute_threads(self: &Arc<Self>) -> Vec<ThreadJoinHandle<Result<()>>> {
        let mut thread_join_handles = Vec::with_capacity(self.threads_num);
        for thread_num in 0..self.threads_num {
            let this = self.clone();
            #[allow(unused_mut)]
            let mut name = Some(format!("PipelineTask-{}", thread_num));

            thread_join_handles.push(Thread::named_spawn(name, move || unsafe {
                // let this_clone = this.clone();

                // let try_result = catch_unwind(move || -> Result<()> {
                //     match this_clone.execute_single_thread(thread_num) {
                //         Ok(_) => Ok(()),
                //         Err(cause) => Err(cause),
                //     }
                // });

                // // finish the pipeline executor when has error or panic
                // if let Err(cause) = try_result.flatten() {
                //     // this.finish(Some(cause));
                // }

                Ok(())
            }));
        }
        thread_join_handles
    }

    unsafe fn execute_single_thread(&self, thread_num: usize) -> Result<()> {
        Ok(())
    }

    fn complete_pipeline(&self) {}

    fn finish_task(&self) -> Result<()> {
        Ok(())
    }
}

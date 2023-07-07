use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::anyhow;
use anyhow::Result;
use futures::{future::BoxFuture, Future};

pub enum TaskExecutionMode {
    Complete,
    Partial,
}

pub enum TaskExecutionResult {
    Finished,
    NotFinished,
    Error,
    Blocked,
}

#[async_trait::async_trait]
pub trait Task {
    /// The name of task.
    fn name(&self) -> String;

    /// Execute the task in the specified execution mode
    /// * If mode is Complete, Execute should always finish processing and
    ///   return Finished
    /// * If mode is Partial, Execute can return not_finished, in which case
    ///   Execute will be called again
    /// * In case of an error, error is returned
    /// * In case the task has interrupted, blocked is returned.
    async fn execute(&mut self, mode: TaskExecutionMode) -> Result<TaskExecutionResult>;
}

pub struct ExecutionTask {
    inner: BoxFuture<'static, Result<()>>,
}

impl ExecutionTask {
    // pub fn create<Inner>(inner: Inner) -> ExecutionTask
    // where
    //     Inner: Future<Output = Result<()>> + Send + 'static,
    // {
    //     ExecutionTask {
    //         inner: inner.boxed(),
    //     }
    // }

    pub fn create(inner: Arc<dyn Task>) -> ExecutionTask {
        todo!()
    }

    pub fn empty() -> ExecutionTask {
        todo!()
    }
}

impl Future for ExecutionTask {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.as_mut();
        let try_result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || -> Poll<Result<()>> {
                inner.poll(cx)
            }));

        match try_result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(_res)) => {
                // self.queue.completed_async_task(
                //     self.workers_condvar.clone(),
                //     CompletedAsyncTask::create(self.processor_id, self.worker_id, res),
                // );
                Poll::Ready(())
            }
            Err(cause) => {
                let _res: Result<()> = match cause.downcast_ref::<&'static str>() {
                    Some(msg) => Err(anyhow!(msg.to_string())),
                    None => match cause.downcast_ref::<String>() {
                        Some(msg) => Err(anyhow!(msg.to_string())),
                        None => Err(anyhow!("unknown panic message".to_string())),
                    },
                };

                // self.queue.completed_async_task(
                //     self.workers_condvar.clone(),
                //     CompletedAsyncTask::create(self.processor_id, self.worker_id, res),
                // );
                Poll::Ready(())
            }
        }
    }
}

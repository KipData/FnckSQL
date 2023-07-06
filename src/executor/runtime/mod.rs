mod catch_unwind;
mod thread;
mod thread_pool;

use anyhow::anyhow;
use anyhow::Result;
use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{
    runtime::Handle,
    sync::{oneshot, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};

use self::catch_unwind::CatchUnwindFuture;

pub trait TrySpawn {
    #[track_caller]
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static;

    #[track_caller]
    fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.try_spawn(task).unwrap()
    }
}

impl<S: TrySpawn> TrySpawn for Arc<S> {
    #[track_caller]
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.as_ref().try_spawn(task)
    }

    #[track_caller]
    fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.as_ref().spawn(task)
    }
}

/// Simple tokio runtime wrapper.
pub struct ExecutionRuntime {
    handle: Handle,

    /// Use to receive a drop signal when dropper is dropped.
    _dropper: Dropper,
}

impl ExecutionRuntime {
    fn builder() -> tokio::runtime::Builder {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all().thread_stack_size(20 * 1024 * 1024);

        builder
    }

    pub fn with_default_worker_threads() -> Result<Self> {
        let rt = Self::builder()
            .build()
            .map_err(|e| anyhow!(e.to_string()))?;
        let (send_stop, recv_stop) = oneshot::channel();

        let handle = rt.handle().clone();
        let join_handler = std::thread::spawn(move || {
            let _ = rt.block_on(recv_stop);
            let instant = Instant::now();

            rt.shutdown_timeout(Duration::from_secs(3));

            instant.elapsed() >= Duration::from_secs(3)
        });

        Ok(ExecutionRuntime {
            handle,
            _dropper: Dropper {
                close: Some(send_stop),
                name: Some("UnnamedRuntime".to_owned()),
                join_handler: Some(join_handler),
            },
        })
    }

    pub fn with_work_threads(num_thread: usize, thread_name: Option<String>) -> Result<Self> {
        let mut rt_builder = Self::builder();
        rt_builder.worker_threads(num_thread);

        if let Some(thread_name) = thread_name.as_ref() {
            rt_builder.thread_name(thread_name);
        }

        let rt = rt_builder.build().map_err(|e| anyhow!(e.to_string()))?;
        let (send_stop, recv_stop) = oneshot::channel();

        let handle = rt.handle().clone();
        let join_handler = std::thread::spawn(move || {
            let _ = rt.block_on(recv_stop);
            let instant = Instant::now();

            rt.shutdown_timeout(Duration::from_secs(3));

            instant.elapsed() >= Duration::from_secs(3)
        });

        Ok(ExecutionRuntime {
            handle,
            _dropper: Dropper {
                close: Some(send_stop),
                name: Some(format!(
                    "{}Runtime",
                    thread_name.unwrap_or("Unnamed".to_owned())
                )),
                join_handler: Some(join_handler),
            },
        })
    }

    pub fn inner(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }

    pub fn block_on<T, F>(&self, future: F) -> F::Output
    where
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let future = CatchUnwindFuture::create(future);
        self.handle.block_on(future).flatten()
    }

    pub async fn try_spawn_batch<Fut>(
        &self,
        semaphore: Semaphore,
        futures: impl IntoIterator<Item = Fut>,
    ) -> Result<Vec<JoinHandle<Fut::Output>>>
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let semaphore = Arc::new(semaphore);
        let iter = futures.into_iter().map(|v| {
            |permit| async {
                let r = v.await;
                drop(permit);
                r
            }
        });
        self.try_spawn_batch_with_owned_semaphore(semaphore, iter)
            .await
    }

    pub async fn try_spawn_batch_with_owned_semaphore<F, Fut>(
        &self,
        semaphore: Arc<Semaphore>,
        futures: impl IntoIterator<Item = F>,
    ) -> Result<Vec<JoinHandle<Fut::Output>>>
    where
        F: FnOnce(OwnedSemaphorePermit) -> Fut + Send + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let iter = futures.into_iter();
        let mut handlers =
            Vec::with_capacity(iter.size_hint().1.unwrap_or_else(|| iter.size_hint().0));

        for fut in iter {
            let semaphore = semaphore.clone();

            let permit = semaphore
                .acquire_owned()
                .await
                .map_err(|e| anyhow!(format!("semaphore closed, acquire permit failure. {}", e)))?;

            let handler = self
                .handle
                .spawn(async_backtrace::location!().frame(async move { fut(permit).await }));
            handlers.push(handler)
        }
        Ok(handlers)
    }
}

impl TrySpawn for ExecutionRuntime {
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        Ok(self.handle.spawn(async_backtrace::location!().frame(task)))
    }
}

struct Dropper {
    name: Option<String>,
    close: Option<oneshot::Sender<()>>,
    join_handler: Option<std::thread::JoinHandle<bool>>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        if let Some(name) = self.name.take() {
            if let Some(close) = self.close.take() {
                let _ = close.send(());
            }

            if let Some(join_handler) = self.join_handler.take() {
                if join_handler.join().unwrap_or(false) {
                    tracing::warn!("{} shutdown timeout", name);
                } else {
                    tracing::info!("{} shutdown success", name);
                }
            }
        }
    }
}

pub async fn execute_futures_in_parallel<Fut>(
    futures: impl IntoIterator<Item = Fut>,
    thread_nums: usize,
    permit_nums: usize,
    thread_name: String,
) -> Result<Vec<Fut::Output>>
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    let semaphore = Semaphore::new(permit_nums);
    let runtime = Arc::new(ExecutionRuntime::with_work_threads(
        thread_nums,
        Some(thread_name),
    )?);

    let join_handlers = runtime.try_spawn_batch(semaphore, futures).await?;
    futures::future::try_join_all(join_handlers)
        .await
        .map_err(|err| anyhow!(format!("try join all futures failure, {}", err)))
}

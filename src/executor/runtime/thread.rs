use anyhow::anyhow;
use anyhow::Result;
use std::thread::{Builder, JoinHandle};
pub struct Thread;

pub struct ThreadJoinHandle<T> {
    inner: JoinHandle<T>,
}

impl<T> ThreadJoinHandle<T> {
    pub fn create(inner: JoinHandle<T>) -> Self {
        ThreadJoinHandle { inner }
    }

    pub fn join(self) -> Result<T> {
        match self.inner.join() {
            Ok(res) => Ok(res),
            Err(cause) => match cause.downcast_ref::<&'static str>() {
                Some(msg) => Err(anyhow!(msg.to_string())),
                None => match cause.downcast_ref::<String>() {
                    Some(msg) => Err(anyhow!(msg.to_string())),
                    None => Err(anyhow!("unknown panic message".to_string())),
                },
            },
        }
    }
}

impl Thread {
    pub fn named_spawn<F, T>(mut name: Option<String>, f: F) -> ThreadJoinHandle<T>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        let mut thread_builder = Builder::new();

        if let Some(named) = name.take() {
            thread_builder = thread_builder.name(named);
        }

        ThreadJoinHandle::create(thread_builder.spawn(f).unwrap())
    }

    pub fn spawn<F, T>(f: F) -> ThreadJoinHandle<T>
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send + 'static,
    {
        Self::named_spawn(None, f)
    }
}

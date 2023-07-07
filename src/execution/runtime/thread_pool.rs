use async_channel::{Receiver, Sender};

use super::thread::Thread;
use anyhow::Result;

pub struct ThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

pub struct TaskJoinHandler<T: Send> {
    rx: Receiver<T>,
}

impl<T: Send> TaskJoinHandler<T> {
    pub fn create(rx: Receiver<T>) -> TaskJoinHandler<T> {
        TaskJoinHandler::<T> { rx }
    }

    pub fn join(&self) -> T {
        self.rx.recv_blocking().unwrap()
    }
}

impl ThreadPool {
    pub fn create(threads: usize) -> Result<ThreadPool> {
        let (tx, rx) = async_channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();

        for _ in 0..threads {
            let thread_rx = rx.clone();
            Thread::spawn(move || {
                while let Ok(task) = thread_rx.recv_blocking() {
                    task();
                }
            });
        }

        Ok(ThreadPool { tx })
    }

    pub fn execute<F, R>(&self, f: F) -> TaskJoinHandler<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = async_channel::bounded(1);
        let _ = self.tx.send_blocking(Box::new(move || {
            let _ = tx.send_blocking(f());
        }));
        TaskJoinHandler::create(rx)
    }
}

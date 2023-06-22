use std::{
    thread,
    time::{Duration, Instant},
};

use anyhow::Result;
use tokio::{runtime::Handle, sync::oneshot};

/// Simple tokio runtime wrapper.
pub struct ExecuteRuntime {
    handle: Handle,
    _dropper: Dropper,
}

impl ExecuteRuntime {
    fn builder() -> tokio::runtime::Builder {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all().thread_stack_size(20 * 1024 * 1024);

        builder
    }

    pub fn with_default_worker_threads() -> Result<Self> {
        let rt = Self::builder()
            .build()
            .map_err(|e| anyhow::Error::msg(e.to_string()))?;
        let (send_stop, recv_stop) = oneshot::channel();

        let handle = rt.handle().clone();
        let join_handler = thread::spawn(move || {
            let _ = rt.block_on(recv_stop);
            let instant = Instant::now();

            rt.shutdown_timeout(Duration::from_secs(3));

            instant.elapsed() >= Duration::from_secs(3)
        });

        Ok(ExecuteRuntime {
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

        let rt = rt_builder
            .build()
            .map_err(|e| anyhow::Error::msg(e.to_string()))?;
        let (send_stop, recv_stop) = oneshot::channel();

        let handle = rt.handle().clone();
        let join_handler = thread::spawn(move || {
            let _ = rt.block_on(recv_stop);
            let instant = Instant::now();

            rt.shutdown_timeout(Duration::from_secs(3));

            instant.elapsed() >= Duration::from_secs(3)
        });

        Ok(ExecuteRuntime {
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
}

struct Dropper {
    name: Option<String>,
    close: Option<oneshot::Sender<()>>,
    join_handler: Option<thread::JoinHandle<bool>>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        if let Some(name) = self.name.take() {
            if let Some(close) = self.close.take() {
                let _ = close.send(());
            }

            if let Some(join_handler) = self.join_handler.take() {
                if join_handler.join().unwrap_or(false) {
                    log::warn!("{} shutdown timeout", name);
                } else {
                    log::info!("{} shutdown success", name);
                }
            }
        }
    }
}

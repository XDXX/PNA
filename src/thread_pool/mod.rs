mod naive;
mod rayon;
mod shared_queue;

pub use self::naive::NaiveThreadPool;
pub use self::rayon::RayonThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;
use crate::Result;

/// An interface for representing the thread pool.
pub trait ThreadPool {
    /// Creates a new thread pool with the specified number of threads.
    fn new(threads: usize) -> Result<Self>
    where
        Self: Sized;

    /// Spawn a function into the thread pool.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

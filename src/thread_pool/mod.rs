use crate::Result;

mod naive;

/// An interface for repersenting the thread pool.
pub trait ThreadPool {
    /// Creates a new thread pool with the specified number of threads.
    fn new(threads: u32) -> Result<Self> where Self: Sized;

    /// Spawn a function into the thread pool.
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

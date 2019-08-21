use std::thread;

use super::ThreadPool;
use crate::Result;

pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_: usize) -> Result<NaiveThreadPool> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F: FnOnce() + Send + 'static>(&self, job: F) {
        thread::spawn(job);
    }
}

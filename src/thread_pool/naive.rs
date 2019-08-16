use std::thread;

use super::ThreadPool;
use crate::Result;

struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_: u32) -> Result<NaiveThreadPool> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F: FnOnce() + Send + 'static>(&self, job: F) {
        thread::spawn(job);
    }
}

use rayon;

use super::ThreadPool;
use crate::Result;

pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: usize) -> Result<RayonThreadPool> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        Ok(RayonThreadPool { pool })
    }

    fn spawn<F: FnOnce() + Send + 'static>(&self, job: F) {
        self.pool.spawn(job);
    }
}

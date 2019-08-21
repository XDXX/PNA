use crossbeam_channel::{unbounded, Receiver, Sender};
use std::thread;

use super::ThreadPool;
use crate::Result;

pub struct SharedQueueThreadPool {
    sender: Sender<Job>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: usize) -> Result<Self>
    where
        Self: Sized,
    {
        assert!(threads > 0);
        let (sender, receiver) = unbounded();

        for _ in 0..threads {
            let receiver = JobReceiver {
                receiver: receiver.clone(),
            };
            thread::spawn(move || {
                while let Ok(job) = receiver.receiver.recv() {
                    job();
                }
            });
        }
        Ok(SharedQueueThreadPool { sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Box::new(job)).unwrap();
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone)]
struct JobReceiver {
    receiver: Receiver<Job>,
}

impl Drop for JobReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let receiver = self.clone();
            thread::spawn(move || {
                while let Ok(job) = receiver.receiver.recv() {
                    job();
                }
            });
        }
    }
}

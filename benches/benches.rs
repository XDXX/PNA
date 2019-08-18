use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use tempfile::TempDir;

use criterion::{criterion_group, criterion_main, Criterion};

const SET_REPEATS: usize = 10;
const GET_REPEATS: usize = 10000;

/// Benchmarking the performance of setting key to database. Note that when collecting
/// multiple samples of `set_kvs`, it may trigger the log compacting.
fn set_bench(c: &mut Criterion) {
    let temp_dir_kvs = TempDir::new().unwrap();
    let mut kv_store = KvStore::open(&temp_dir_kvs).unwrap();

    let temp_dir_sled = TempDir::new().unwrap();
    let mut sled_db = SledKvsEngine::open(&temp_dir_sled).unwrap();

    c.bench_function("set_kvs", move |b| {
        b.iter(|| set_n_times(&mut kv_store, SET_REPEATS))
    });

    c.bench_function("set_sled", move |b| {
        b.iter(|| set_n_times(&mut sled_db, SET_REPEATS))
    });
}

fn get_bench(c: &mut Criterion) {
    let temp_dir_kvs = TempDir::new().unwrap();
    let mut kv_store = KvStore::open(&temp_dir_kvs).unwrap();
    let temp_dir_sled = TempDir::new().unwrap();
    let mut sled_db = SledKvsEngine::open(&temp_dir_sled).unwrap();

    set_n_times(&mut kv_store, GET_REPEATS);
    set_n_times(&mut sled_db, GET_REPEATS);

    c.bench_function("get_kvs", move |b| {
        b.iter(|| get_n_times_randomly(&mut kv_store, GET_REPEATS))
    });

    c.bench_function("get_sled", move |b| {
        b.iter(|| get_n_times_randomly(&mut sled_db, GET_REPEATS))
    });
}

fn set_n_times<E: KvsEngine>(engine: &mut E, n: usize) {
    for i in 0..n {
        engine
            .set(format!("key{}", i), "value".to_string())
            .unwrap();
    }
}

fn get_n_times_randomly<E: KvsEngine>(engine: &mut E, n: usize) {
    let mut rng = SmallRng::from_seed([6; 16]);
    for _ in 0..n {
        engine
            .get(format!("key{}", rng.gen_range(0, GET_REPEATS)))
            .unwrap();
    }
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);

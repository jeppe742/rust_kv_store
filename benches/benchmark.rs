use std::{fs::remove_dir_all, path::PathBuf};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rust_kv_store::db::db::DB;

use rand::Rng;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple benchmark");
    let path = PathBuf::from("./benches/output/insert");

    let mut db = DB::new(&path);
    let mut rng = rand::thread_rng();
    group.bench_function("insert", |b| {
        b.iter(|| {
            black_box(
                db.set(rng.gen::<u32>().to_string(), rng.gen::<u32>().to_string())
                    .unwrap(),
            )
        })
    });

    group.bench_function("read", |b| {
        b.iter(|| black_box(db.get(&rng.gen::<u32>().to_string())))
    });

    remove_dir_all(path).unwrap();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

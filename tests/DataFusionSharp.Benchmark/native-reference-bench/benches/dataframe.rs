use datafusion::prelude::SessionContext;
use divan::{Bencher, black_box};

fn main() {
    divan::main();
}

const ROWS_COUNTS: &[u32] = &[1, 100, 10000, 1000000];

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap()
}

fn generate_dataframe(runtime: &tokio::runtime::Runtime, row_count: u32) -> datafusion::prelude::DataFrame {
    runtime.block_on(async move {
        let session_context = SessionContext::new();
        let sql = format!("SELECT series.value AS id FROM generate_series(1, {row_count}) AS series");
        session_context.sql(&sql).await.unwrap()
    })
}

#[divan::bench(args = ROWS_COUNTS)]
fn count(bencher: Bencher, row_count: u32) {
    let runtime = create_runtime();
    let df = generate_dataframe(&runtime, row_count);

    bencher.bench_local(move || {
        let df = df.clone();
        runtime.block_on(async move {
            let result = df.count().await.unwrap();
            black_box(result);
        });
    });
}

#[divan::bench(args = ROWS_COUNTS)]
fn get_schema(bencher: Bencher, row_count: u32) {
    let runtime = create_runtime();
    let df = generate_dataframe(&runtime, row_count);

    bencher.bench_local(move || {
        let result = df.schema();
        black_box(result);
    });
}

#[divan::bench(args = ROWS_COUNTS)]
fn collect(bencher: Bencher, row_count: u32) {
    let runtime = create_runtime();
    let df = generate_dataframe(&runtime, row_count);

    bencher.bench_local(move || {
        let df = df.clone();
        runtime.block_on(async move {
            let result = df.collect().await.unwrap();
            black_box(result);
        });
    });
}
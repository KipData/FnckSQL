use criterion::{criterion_group, criterion_main, Criterion};
use fnck_sql::db::DataBaseBuilder;
use fnck_sql::errors::DatabaseError;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
#[cfg(unix)]
use pprof::criterion::{Output, PProfProfiler};
use sqlite::Error;
use std::fs;
use std::path::Path;

const QUERY_BENCH_FNCK_SQL_PATH: &'static str = "./fncksql_bench";
const QUERY_BENCH_SQLITE_PATH: &'static str = "./sqlite_bench";
const TABLE_ROW_NUM: u64 = 2_00_000;

fn query_cases() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Full  Read", "select * from t1"),
        ("Point Read", "select * from t1 where c1 = 1000"),
        (
            "Range Read",
            "select * from t1 where c1 > 500 and c1 < 1000",
        ),
    ]
}

async fn init_fncksql_query_bench() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path(QUERY_BENCH_FNCK_SQL_PATH)
        .build()
        .await
        .unwrap();
    database
        .run("create table t1 (c1 int primary key, c2 int)")
        .await?;
    let pb = ProgressBar::new(TABLE_ROW_NUM);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}")
            .unwrap(),
    );
    for i in 0..TABLE_ROW_NUM {
        let _ = database
            .run(format!("insert into t1 values({}, {})", i, i + 1).as_str())
            .await?;
        pb.set_position(i + 1);
    }
    pb.finish_with_message("Insert completed!");

    let _ = database.run("analyze table t1").await?;

    Ok(())
}

fn init_sqlite_query_bench() -> Result<(), Error> {
    let connection = sqlite::open(QUERY_BENCH_SQLITE_PATH.to_owned())?;

    let _ = connection.execute("create table t1 (c1 int primary key, c2 int)")?;

    let pb = ProgressBar::new(TABLE_ROW_NUM);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}")
            .unwrap(),
    );
    for i in 0..TABLE_ROW_NUM {
        let _ = connection.execute(format!("insert into t1 values({}, {})", i, i + 1))?;
        pb.set_position(i + 1);
    }
    pb.finish_with_message("Insert completed!");

    Ok(())
}

fn path_exists_and_is_directory(path: &str) -> bool {
    match fs::metadata(path) {
        Ok(metadata) => metadata.is_dir(),
        Err(_) => false,
    }
}

fn query_on_execute(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let database = rt.block_on(async {
        if !Path::new(QUERY_BENCH_SQLITE_PATH).exists() {
            println!(
                "SQLITE: The table is not initialized and data insertion is started. => {}",
                TABLE_ROW_NUM
            );

            init_sqlite_query_bench().unwrap();
        }
        if !path_exists_and_is_directory(QUERY_BENCH_FNCK_SQL_PATH) {
            println!(
                "FnckSQL: The table is not initialized and data insertion is started. => {}",
                TABLE_ROW_NUM
            );

            init_fncksql_query_bench().await.unwrap();
        }
        DataBaseBuilder::path(QUERY_BENCH_FNCK_SQL_PATH)
            .build()
            .await
            .unwrap()
    });
    println!("Table initialization completed");

    for (name, case) in query_cases() {
        c.bench_function(format!("FnckSQL: {} by '{}'", name, case).as_str(), |b| {
            b.to_async(&rt).iter(|| async {
                let _tuples = database.run(case).await.unwrap();
            })
        });

        let connection = sqlite::open(QUERY_BENCH_SQLITE_PATH.to_owned()).unwrap();
        c.bench_function(format!("SQLite: {} by '{}'", name, case).as_str(), |b| {
            b.to_async(&rt).iter(|| async {
                let _tuples = connection
                    .prepare(case)
                    .unwrap()
                    .into_iter()
                    .map(|row| row.unwrap())
                    .collect_vec();
            })
        });
    }
}

#[cfg(unix)]
criterion_group!(
    name = query_benches;
    config = Criterion::default().sample_size(10).with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = query_on_execute
);
#[cfg(windows)]
criterion_group!(
    name = query_benches;
    config = Criterion::default().sample_size(10);
    targets = query_on_execute
);

criterion_main!(query_benches,);

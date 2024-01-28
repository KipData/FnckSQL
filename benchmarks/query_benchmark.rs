use criterion::{criterion_group, criterion_main, Criterion};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use fnck_sql::db::{Database, DatabaseError};
use fnck_sql::execution::volcano;
use fnck_sql::storage::kip::KipStorage;
use fnck_sql::storage::Storage;
use sqlite::Error;
use std::cell::RefCell;
use std::fs;
use std::path::Path;
use std::sync::Arc;

const QUERY_CASE: &'static str = "select * from t1 where c1 = 1000";
const QUERY_BENCH_FNCK_SQL_PATH: &'static str = "./fncksql_bench";
const QUERY_BENCH_SQLITE_PATH: &'static str = "./sqlite_bench";
const TABLE_ROW_NUM: u64 = 2_00_000;

async fn init_fncksql_query_bench() -> Result<(), DatabaseError> {
    let database = Database::with_kipdb(QUERY_BENCH_FNCK_SQL_PATH).await.unwrap();
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

        Database::<KipStorage>::with_kipdb(QUERY_BENCH_FNCK_SQL_PATH)
            .await
            .unwrap()
    });

    println!("Table initialization completed");

    #[cfg(feature = "codegen_execute")]
    {
        use kip_sql::execution::codegen;

        let (codegen_transaction, plan) = rt.block_on(async {
            let transaction = database.storage.transaction().await.unwrap();
            let (plan, _) = Database::<KipStorage>::build_plan(QUERY_CASE, &transaction).unwrap();

            (Arc::new(transaction), plan)
        });

        c.bench_function(format!("Codegen: {}", QUERY_CASE).as_str(), |b| {
            b.to_async(&rt).iter(|| async {
                let tuples = codegen::execute(plan.clone(), codegen_transaction.clone())
                    .await
                    .unwrap();
                if tuples.len() as u64 != TABLE_ROW_NUM {
                    panic!("{}", tuples.len());
                }
            })
        });

        let (volcano_transaction, plan) = rt.block_on(async {
            let transaction = database.storage.transaction().await.unwrap();
            let (plan, _) = Database::<KipStorage>::build_plan(QUERY_CASE, &transaction).unwrap();

            (RefCell::new(transaction), plan)
        });

        c.bench_function(format!("Volcano: {}", QUERY_CASE).as_str(), |b| {
            b.to_async(&rt).iter(|| async {
                let mut stream = volcano::build_stream(plan.clone(), &volcano_transaction);
                let tuples = volcano::try_collect(&mut stream).await.unwrap();
                if tuples.len() as u64 != TABLE_ROW_NUM {
                    panic!("{}", tuples.len());
                }
            })
        });
    }

    c.bench_function(format!("FnckSQL: {}", QUERY_CASE).as_str(), |b| {
        b.to_async(&rt).iter(|| async {
            let _tuples = database.run(QUERY_CASE).await.unwrap();
        })
    });

    let connection = sqlite::open(QUERY_BENCH_SQLITE_PATH.to_owned()).unwrap();
    c.bench_function(format!("SQLite: {}", QUERY_CASE).as_str(), |b| {
        b.to_async(&rt).iter(|| async {
            let _tuples = connection
                .prepare(QUERY_CASE)
                .unwrap()
                .into_iter()
                .map(|row| row.unwrap())
                .collect_vec();
        })
    });
}

criterion_group!(
    name = query_benches;
    config = Criterion::default().sample_size(10);
    targets = query_on_execute
);

criterion_main!(query_benches,);

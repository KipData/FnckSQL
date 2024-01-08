use criterion::{criterion_group, criterion_main, Criterion};
use indicatif::{ProgressBar, ProgressStyle};
use kip_sql::db::{Database, DatabaseError};
use kip_sql::execution::{codegen, volcano};
use kip_sql::storage::kip::KipStorage;
use kip_sql::storage::Storage;
use std::cell::RefCell;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const QUERY_BENCH_PATH: &'static str = "./query_bench_data";
const TABLE_ROW_NUM: u64 = 2_00_000;

async fn init_query_bench() -> Result<(), DatabaseError> {
    let database = Database::with_kipdb(QUERY_BENCH_PATH).await.unwrap();
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
        if !path_exists_and_is_directory(QUERY_BENCH_PATH) {
            println!(
                "The table is not initialized and data insertion is started. => {}",
                TABLE_ROW_NUM
            );

            init_query_bench().await.unwrap();
        }

        Database::<KipStorage>::with_kipdb(QUERY_BENCH_PATH)
            .await
            .unwrap()
    });

    println!("Table initialization completed");

    let (codegen_transaction, plan) = rt.block_on(async {
        let transaction = database.storage.transaction().await.unwrap();
        let (plan, _) =
            Database::<KipStorage>::build_plan("select * from t1", &transaction).unwrap();

        (Arc::new(transaction), plan)
    });

    c.bench_function("Codegen: select all", |b| {
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
        let (plan, _) =
            Database::<KipStorage>::build_plan("select * from t1", &transaction).unwrap();

        (RefCell::new(transaction), plan)
    });

    c.bench_function("Volcano: select all", |b| {
        b.to_async(&rt).iter(|| async {
            let mut stream = volcano::build_stream(plan.clone(), &volcano_transaction);
            let tuples = volcano::try_collect(&mut stream).await.unwrap();
            if tuples.len() as u64 != TABLE_ROW_NUM {
                panic!("{}", tuples.len());
            }
        })
    });
}

criterion_group!(
    name = query_benches;
    config = Criterion::default().sample_size(10);
    targets = query_on_execute
);

criterion_main!(query_benches,);

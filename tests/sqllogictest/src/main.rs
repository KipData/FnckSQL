use fnck_sql::db::Database;
use sqllogictest::Runner;
use sqllogictest_test::KipSQL;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

#[tokio::main]
async fn main() {
    const SLT_PATTERN: &str = "tests/slt/**/*.slt";

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    std::env::set_current_dir(path).unwrap();

    println!("FnckSQL Test Start!\n");
    init_20000_row_csv().expect("failed to init csv");

    for slt_file in glob::glob(SLT_PATTERN).expect("failed to find slt files") {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let filepath = slt_file
            .expect("failed to read slt file")
            .to_str()
            .unwrap()
            .to_string();
        println!("-> Now the test file is: {}", filepath);

        let db = Database::with_kipdb(temp_dir.path())
            .await
            .expect("init db error");
        let mut tester = Runner::new(KipSQL { db });

        if let Err(err) = tester.run_file_async(filepath).await {
            panic!("test error: {}", err);
        }
        println!("-> Pass!\n\n")
    }
}

fn init_20000_row_csv() -> io::Result<()> {
    let path = "tests/data/row_20000.csv";

    if !Path::new(path).exists() {
        let mut file = File::create(path)?;

        for i in 0..20_000 {
            let row = (0..3)
                .map(|j| (i * 3 + j).to_string())
                .collect::<Vec<_>>()
                .join("|");
            writeln!(file, "{}", row)?;
        }
    }

    Ok(())
}

use kip_sql::db::Database;
use sqllogictest::Runner;
use sqllogictest_test::KipSQL;
use std::path::Path;
use tempfile::TempDir;

#[tokio::main]
async fn main() {
    const SLT_PATTERN: &str = "tests/slt/**/*.slt";

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    std::env::set_current_dir(path).unwrap();

    println!("KipSQL Test Start!\n");

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

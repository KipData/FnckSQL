use std::path::Path;

use libtest_mimic::{Arguments, Trial};
use sqllogictest_test::{test_run};

fn main() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    std::env::set_current_dir(path).unwrap();

    const SLT_PATTERN: &str = "tests/slt/**/*.slt";

    let args = Arguments::from_args();
    let mut tests = vec![];

    let slt_files = glob::glob(SLT_PATTERN).expect("failed to find slt files");
    for slt_file in slt_files {
        let filepath = slt_file.expect("failed to read slt file");
        let filename = filepath
            .file_stem()
            .expect("failed to get file name")
            .to_str()
            .unwrap()
            .to_string();
        let filepath = filepath.to_str().unwrap().to_string();

        let test = Trial::test(filename, move || {
            test_run(filepath.as_str());
            Ok(())
        });

        tests.push(test);
    }

    libtest_mimic::run(&args, tests).exit();
}
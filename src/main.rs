use std::io;

use kip_sql::db::Database;
use kip_sql::storage::Storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(":) Welcome to the KIPSQL, Please input sql.");

    let mut db = Database::new_on_mem();
    loop {
        println!("> ");
        println!("RootCatalog: {:?}", db.storage.get_catalog());
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let ret = db.run(&input);
        println!("{:?}", ret);
    }
}

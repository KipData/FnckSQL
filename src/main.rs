use kip_sql::db::Database;
use std::io;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(":) Welcome to the KIPSQL, Please input sql.");

    let mut db = Database::new();
    loop {
        println!("database catalog{:?}", db.catalog);
        println!("storage  catalog{:?}", db.storage.catalog);
        println!("> ");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let ret = db.run(&input);
        println!("{:?}", ret);
    }
}

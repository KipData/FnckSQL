use std::error::Error;
use std::io;

use kip_sql::db::Database;
use kip_sql::types::tuple::create_table;

pub(crate) const BANNER: &str = "
██╗  ██╗██╗██████╗ ███████╗ ██████╗ ██╗
██║ ██╔╝██║██╔══██╗██╔════╝██╔═══██╗██║
█████╔╝ ██║██████╔╝███████╗██║   ██║██║
██╔═██╗ ██║██╔═══╝ ╚════██║██║▄▄ ██║██║
██║  ██╗██║██║     ███████║╚██████╔╝███████╗
╚═╝  ╚═╝╚═╝╚═╝     ╚══════╝ ╚══▀▀═╝ ╚══════╝";

pub const BLOOM: &str ="
          _ ._  _ , _ ._
        (_ ' ( `  )_  .__)
      ( (  (    )   `)  ) _)
- --=(;;(----(-----)-----);;)==-- -
     (__ (_   (_ . _) _) ,__)
         `~~`\\ ' . /`~~`
              ;   ;
              /   \\
_____________/_ __ \\_____________

Bloom!!!! say goodbye to your data :)
";
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("{} \nVersion: {}\n", BANNER, env!("CARGO_PKG_VERSION"));

    println!(":) Welcome to the KipSQL, Please input sql.\n");
    println!("Tips: ");
    println!("1. input \"quit\" to shutdown");
    println!("2. shutdown will let you say goodbye to your data\n");

    server_run().await?;

    Ok(())
}

async fn server_run() -> Result<(), Box<dyn Error>> {
    let db = Database::new_on_mem();
    let _ = db.run("create table t1 (a int, b int)").await?;
    let _ = db.run("create table t2 (c int, d int null)").await?;
    let _ = db.run("insert into t1 (a, b) values (1, 1), (5, 3), (5, 2)").await?;
    let _ = db.run("insert into t2 (d, c) values (2, 1), (3, 1), (null, 6)").await?;

    loop {
        println!("> type👇 plz");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if input.eq("\n") {
            continue
        }

        if input.to_lowercase()[..4].eq("quit") {
            println!("{}", BLOOM);
            break
        }

        match db.run(&input).await {
            Ok(tuples) => {
                if tuples.is_empty() {
                    println!("\nEmpty!");
                } else {
                    println!("\n{}", create_table(&tuples));
                }
            }
            Err(err) => {
                println!("{}", err);
            }
        }
    }

    Ok(())
}
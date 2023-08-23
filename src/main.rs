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
    loop {
        println!("> type👇 plz");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if input.to_lowercase()[..4].eq("quit") {
            println!("{}", BLOOM);
            break
        }

        let tuples = db.run(&input).await?;

        if tuples.is_empty() {
            println!("\nEmpty!");
        } else {
            println!("\n{}", create_table(&tuples));
        }
    }

    Ok(())
}
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
";
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("{} \nVersion: {}\n", BANNER, env!("CARGO_PKG_VERSION"));

    println!(":) Welcome to the KipSQL, Please input sql.\n");
    println!("Tips: ");
    println!("1. input \"quit\" to shutdown");
    println!("2. no support \"delete\", so if u want remove data, you can delete the \'data\' folder");

    server_run().await?;

    Ok(())
}

async fn server_run() -> Result<(), Box<dyn Error>> {
    let db = Database::new("./data").await?;

    loop {
        println!("> type👇 plz");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if input.to_lowercase().eq("quit\n") {
            println!("{}", BLOOM);
            break
        }

        match db.run(&input).await {
            Ok(tuples) => {
                if tuples.is_empty() {
                    println!("\nEmpty\n");
                } else {
                    println!("\n{}\n", create_table(&tuples));
                }
            }
            Err(err) => {
                println!("{}", err);
            }
        }
    }

    Ok(())
}
use clap::Parser;
use kip_sql::db::Database;
use kip_sql::types::tuple::create_table;
use std::error::Error;
use std::{env, io};

pub(crate) const BANNER: &str = "
██╗  ██╗██╗██████╗ ███████╗ ██████╗ ██╗
██║ ██╔╝██║██╔══██╗██╔════╝██╔═══██╗██║
█████╔╝ ██║██████╔╝███████╗██║   ██║██║
██╔═██╗ ██║██╔═══╝ ╚════██║██║▄▄ ██║██║
██║  ██╗██║██║     ███████║╚██████╔╝███████╗
╚═╝  ╚═╝╚═╝╚═╝     ╚══════╝ ╚══▀▀═╝ ╚══════╝";

pub const BLOOM: &str = "
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

/// KipSQL is a embedded database
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to db file
    #[arg(short, long, default_value = "./data")]
    path: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    println!("{} \nVersion: {}\n", BANNER, env!("CARGO_PKG_VERSION"));
    println!(":) Welcome to the KipSQL, Please input sql.\n");
    println!("Tips🔞: ");
    println!("1. input \"quit\" to shutdown");
    println!(
        "2. all data is in the \'{}\' folder in the directory where the application is run",
        args.path
    );
    server_run(args.path).await?;
    Ok(())
}

async fn server_run(path: String) -> Result<(), Box<dyn Error>> {
    let db = Database::with_kipdb(path).await?;

    loop {
        println!("> 👇👇🏻👇🏼👇🏽👇🏾👇🏿 <");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if input.len() >= 4 && input.to_lowercase()[..4].eq("quit") {
            println!("{}", BLOOM);
            break;
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
                println!("Oops!: {}", err);
            }
        }
    }

    Ok(())
}

use clap::Parser;
use fnck_sql::db::Database;
use fnck_sql::types::tuple::create_table;
use std::error::Error;
use std::{env, io};

pub(crate) const BANNER: &str = "
███████╗███╗   ██╗ ██████╗██╗  ██╗    ███████╗ ██████╗ ██╗
██╔════╝████╗  ██║██╔════╝██║ ██╔╝    ██╔════╝██╔═══██╗██║
█████╗  ██╔██╗ ██║██║     █████╔╝     ███████╗██║   ██║██║
██╔══╝  ██║╚██╗██║██║     ██╔═██╗     ╚════██║██║▄▄ ██║██║
██║     ██║ ╚████║╚██████╗██║  ██╗    ███████║╚██████╔╝███████╗
╚═╝     ╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝    ╚══════╝ ╚══▀▀═╝ ╚══════╝

";

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
    println!(":) Welcome to the FnckSQL🖕\n");
    println!("Tips🔞: ");
    println!("1. input \"quit\" to shutdown");
    println!(
        "2. all data is in the \'{}\' folder in the directory where the application is run",
        args.path
    );
    println!("If you want to customize the location of the data directory, please add parameters: \"-- --path './knck_sql/data'\"");
    server_run(args.path).await?;
    Ok(())
}

async fn server_run(path: String) -> Result<(), Box<dyn Error>> {
    let db = Database::with_kipdb(path).await?;

    loop {
        println!("> 🖕🖕🏻🖕🏼🖕🏼🖕🏾🖕🏿 <");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if input.len() >= 4 && input.to_lowercase()[..4].eq("quit") {
            println!("{}", BLOOM);
            break;
        }

        match db.run(&input).await {
            Ok(tuples) => {
                println!("\n{}\nRow len: {}\n", create_table(&tuples), tuples.len());
            }
            Err(err) => {
                println!("Oops!: {}", err);
            }
        }
    }

    Ok(())
}

use std::error::Error;
use std::io;
use tracing::error;

use kip_sql::db::Database;
use kip_sql::types::tuple::create_table;

pub(crate) const BANNER: &str = "
██╗  ██╗██╗██████╗ ███████╗ ██████╗ ██╗
██║ ██╔╝██║██╔══██╗██╔════╝██╔═══██╗██║
█████╔╝ ██║██████╔╝███████╗██║   ██║██║
██╔═██╗ ██║██╔═══╝ ╚════██║██║▄▄ ██║██║
██║  ██╗██║██║     ███████║╚██████╔╝███████╗
╚═╝  ╚═╝╚═╝╚═╝     ╚══════╝ ╚══▀▀═╝ ╚══════╝";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{} \nVersion: {}\n", BANNER, env!("CARGO_PKG_VERSION"));

    println!(":) Welcome to the KipSQL, Please input sql.\n");
    println!("Tips: ");
    println!("1. if the sql is not correct, then i will be very sad :(");
    println!("2. shutdown will let you say goodbye to your data");

    tokio::select! {
        res = server_run() => {
            if let Err(err) = res {
                error!(cause = %err, "Bloom!!!!");
            }
        }
        _ = quit() => {
            println!("Bye! (and you data)");
        }
    }

    Ok(())
}

async fn server_run() -> Result<(), Box<dyn Error>> {
    let db = Database::new_on_mem();
    loop {
        println!("> type👇 plz");
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let tuples = db.run(&input).await?;

        if tuples.is_empty() {
            println!("\nEmpty!");
        } else {
            println!("\n{}", create_table(&tuples));
        }
    }
}

pub async fn quit() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = interrupt.recv() => (),
            _ = terminate.recv() => (),
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        Ok(tokio::signal::ctrl_c().await?)
    }
}
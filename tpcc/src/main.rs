use crate::delivery::DeliveryTest;
use crate::load::Load;
use crate::new_ord::NewOrdTest;
use crate::order_stat::OrderStatTest;
use crate::payment::PaymentTest;
use crate::rt_hist::RtHist;
use crate::slev::SlevTest;
use clap::Parser;
use fnck_sql::db::{DBTransaction, DataBaseBuilder};
use fnck_sql::errors::DatabaseError;
use fnck_sql::storage::Storage;
use rand::prelude::ThreadRng;
use rand::Rng;
use std::time::{Duration, Instant};

mod delivery;
mod load;
mod new_ord;
mod order_stat;
mod payment;
mod rt_hist;
mod slev;

pub(crate) const ALLOW_MULTI_WAREHOUSE_TX: bool = true;

pub(crate) trait TpccTransaction<S: Storage> {
    type Args;

    fn run(tx: &mut DBTransaction<S>, args: &Self::Args) -> Result<(), TpccError>;
}

pub(crate) trait TpccTest<S: Storage> {
    fn name(&self) -> &'static str;

    fn do_transaction(
        &self,
        rng: &mut ThreadRng,
        tx: &mut DBTransaction<S>,
        num_ware: usize,
        args: &TpccArgs,
    ) -> Result<(), TpccError>;
}

struct TpccArgs {
    joins: bool,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "false")]
    joins: bool,
    #[clap(long, default_value = "fnck_sql_tpcc")]
    path: String,
    #[clap(long, default_value = "5")]
    max_retry: usize,
    #[clap(long, default_value = "1080")]
    measure_time: u64,
    #[clap(long, default_value = "1")]
    num_ware: usize,
}

fn main() -> Result<(), TpccError> {
    let args = Args::parse();

    let mut rng = rand::thread_rng();
    let database = DataBaseBuilder::path(&args.path).build()?;

    Load::load_items(&mut rng, &database)?;
    Load::load_warehouses(&mut rng, &database, args.num_ware)?;
    Load::load_custs(&mut rng, &database, args.num_ware)?;
    Load::load_ord(&mut rng, &database, args.num_ware)?;

    let mut rt_hist = RtHist::new();
    let tests = vec![
        Box::new(NewOrdTest) as Box<dyn TpccTest<_>>,
        Box::new(PaymentTest),
        Box::new(OrderStatTest),
        Box::new(DeliveryTest),
        Box::new(SlevTest),
    ];
    let tpcc_args = TpccArgs { joins: args.joins };

    let tpcc_start = Instant::now();
    let duration = Duration::new(args.measure_time, 0);
    let mut round_count = 0;

    while tpcc_start.elapsed() < duration {
        for (i, tpcc_test) in tests.iter().enumerate() {
            let mut is_succeed = false;
            for j in 0..args.max_retry + 1 {
                let transaction_start = Instant::now();
                let mut tx = database.new_transaction()?;

                if let Err(err) =
                    tpcc_test.do_transaction(&mut rng, &mut tx, args.num_ware, &tpcc_args)
                {
                    eprintln!(
                        "[{}] Error while doing transaction: {}",
                        tpcc_test.name(),
                        err
                    );
                } else {
                    rt_hist.hist_inc(i, transaction_start.elapsed());
                    is_succeed = true;
                    break;
                }
                if j < args.max_retry {
                    println!("[{}] Retry for the {}th time", tpcc_test.name(), j + 1);
                }
            }
            if !is_succeed {
                return Err(TpccError::MaxRetry);
            }
        }
        if round_count != 0 && round_count % 4 == 0 {
            println!(
                "============ TPCC CheckPoint {} on round {round_count}: ===============",
                round_count / 4
            );
            for (i, name) in vec![
                "New-Order",
                "Payment",
                "Order-Status",
                "Delivery",
                "Stock-Level",
            ]
            .into_iter()
            .enumerate()
            {
                println!("{name} 90th Percentile RT: {:.3}", rt_hist.hist_ckp(i));
            }
            println!("==========================================================");
        }
        round_count += 1;
    }
    rt_hist.hist_report();

    Ok(())
}

fn other_ware(rng: &mut ThreadRng, home_ware: usize, num_ware: usize) -> usize {
    if num_ware == 1 {
        return home_ware;
    }

    loop {
        let tmp = rng.gen_range(1..num_ware);
        if tmp != home_ware {
            return tmp;
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TpccError {
    #[error("fnck_sql: {0}")]
    Database(
        #[source]
        #[from]
        DatabaseError,
    ),
    #[error("tuples is empty")]
    EmptyTuples,
    #[error("maximum retries reached")]
    MaxRetry,
}

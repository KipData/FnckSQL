use crate::load::{last_name, nu_rand, CUST_PER_DIST, DIST_PER_WARE};
use crate::{other_ware, TpccArgs, TpccError, TpccTest, TpccTransaction, ALLOW_MULTI_WAREHOUSE_TX};
use chrono::Utc;
use fnck_sql::db::DBTransaction;
use fnck_sql::storage::Storage;
use rand::prelude::ThreadRng;
use rand::Rng;
use rust_decimal::Decimal;

#[derive(Debug)]
pub(crate) struct PaymentArgs {
    w_id: usize,
    d_id: usize,
    by_name: bool,
    c_w_id: usize,
    c_d_id: usize,
    c_id: usize,
    c_last: String,
    h_amount: Decimal,
}

impl PaymentArgs {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        w_id: usize,
        d_id: usize,
        by_name: bool,
        c_w_id: usize,
        c_d_id: usize,
        c_id: usize,
        c_last: String,
        h_amount: Decimal,
    ) -> Self {
        Self {
            w_id,
            d_id,
            by_name,
            c_w_id,
            c_d_id,
            c_id,
            c_last,
            h_amount,
        }
    }
}

pub(crate) struct Payment;
pub(crate) struct PaymentTest;

impl<S: Storage> TpccTransaction<S> for Payment {
    type Args = PaymentArgs;

    #[allow(unused_variables)]
    fn run(tx: &mut DBTransaction<S>, args: &Self::Args) -> Result<(), TpccError> {
        // "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?"
        let _ = tx.run(format!(
            "UPDATE warehouse SET w_ytd = w_ytd + {} WHERE w_id = {}",
            args.h_amount, args.w_id
        ))?;

        // "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?"
        let (_, tuples) = tx.run(format!("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = {}", args.w_id))?;
        let w_street_1 = tuples[0].values[0].utf8().unwrap();
        let w_street_2 = tuples[0].values[1].utf8().unwrap();
        let w_city = tuples[0].values[2].utf8().unwrap();
        let w_state = tuples[0].values[3].utf8().unwrap();
        let w_zip = tuples[0].values[4].utf8().unwrap();
        let w_name = tuples[0].values[5].utf8().unwrap();

        // "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?"
        let _ = tx.run(format!(
            "UPDATE district SET d_ytd = d_ytd + {} WHERE d_w_id = {} AND d_id = {}",
            args.h_amount, args.w_id, args.d_id
        ))?;

        // "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?"
        let (_, tuples) = tx.run(format!("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = {} AND d_id = {}", args.w_id, args.d_id))?;
        let d_street_1 = tuples[0].values[0].utf8().unwrap();
        let d_street_2 = tuples[0].values[1].utf8().unwrap();
        let d_city = tuples[0].values[2].utf8().unwrap();
        let d_state = tuples[0].values[3].utf8().unwrap();
        let d_zip = tuples[0].values[4].utf8().unwrap();
        let d_name = tuples[0].values[5].utf8().unwrap();

        let mut c_id = args.c_id as i32;
        if args.by_name {
            // "SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?"
            let (_, tuples) = tx.run(format!("SELECT count(c_id) FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}'", args.c_w_id, args.c_d_id, args.c_last))?;
            let mut name_cnt = tuples[0].values[0].i32().unwrap();

            // "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first"
            let (_, tuples) = tx.run(format!("SELECT c_id FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first", args.c_w_id, args.c_d_id, args.c_last))?;
            if name_cnt % 2 == 1 {
                name_cnt += 1;
            }
            for n in 0..name_cnt / 2 {
                c_id = tuples[n as usize].values[0].i32().unwrap();
            }
        }
        // "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE"
        let (_, tuples) = tx.run(format!("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", args.c_w_id, args.c_d_id, c_id))?;
        let c_first = tuples[0].values[0].utf8().unwrap();
        let c_middle = tuples[0].values[1].utf8().unwrap();
        let c_last = tuples[0].values[2].utf8().unwrap();
        let c_street_1 = tuples[0].values[3].utf8().unwrap();
        let c_street_2 = tuples[0].values[4].utf8().unwrap();
        let c_city = tuples[0].values[5].utf8().unwrap();
        let c_state = tuples[0].values[6].utf8().unwrap();
        let c_zip = tuples[0].values[7].utf8().unwrap();
        let c_phone = tuples[0].values[8].utf8().unwrap();
        let c_credit = tuples[0].values[9].utf8();
        let c_credit_lim = tuples[0].values[10].i64().unwrap();
        let c_discount = tuples[0].values[11].decimal().unwrap();
        let mut c_balance = tuples[0].values[12].decimal().unwrap();
        let c_since = tuples[0].values[13].datetime().unwrap();

        c_balance += args.h_amount;
        if let Some(c_credit) = c_credit {
            if c_credit.contains("BC") {
                // "SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                let (_, tuples) = tx.run(format!(
                    "SELECT c_data FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                    args.c_w_id, args.c_d_id, c_id
                ))?;
                let c_data = tuples[0].values[0].utf8().unwrap();

                // https://github.com/AgilData/tpcc/blob/dfbabe1e35cc93b2bf2e107fc699eb29c2097e24/src/main/java/com/codefutures/tpcc/Payment.java#L284
                // let c_new_data = format!("| {} {} {} {} {} {} {}", c_id, args.c_d_id, args.c_w_id, args.d_id, args.w_id, args.h_amount, )

                // "UPDATE customer SET c_balance = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                let _ = tx.run(format!("UPDATE customer SET c_balance = {}, c_data = '{}' WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_balance, c_data, args.c_w_id, args.c_d_id, c_id))?;
            } else {
                // "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                let _ = tx.run(format!("UPDATE customer SET c_balance = {} WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_balance, args.c_w_id, args.c_d_id, c_id))?;
            }
        } else {
            // "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
            let _ = tx.run(format!("UPDATE customer SET c_balance = {} WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_balance, args.c_w_id, args.c_d_id, c_id))?;
        }
        let h_data = format!("\\0{d_name}    \\0");

        let now = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        // "INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
        let _ = tx.run(format!("INSERT OVERWRITE history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES({}, {}, {}, {}, {}, '{}', {}, '{}')", args.c_d_id, args.c_w_id, c_id, args.d_id, args.w_id, now, args.h_amount, h_data))?;

        Ok(())
    }
}

impl<S: Storage> TpccTest<S> for PaymentTest {
    fn name(&self) -> &'static str {
        "Payment"
    }

    fn do_transaction(
        &self,
        rng: &mut ThreadRng,
        tx: &mut DBTransaction<S>,
        num_ware: usize,
        _: &TpccArgs,
    ) -> Result<(), TpccError> {
        let w_id = rng.gen_range(0..num_ware) + 1;
        let d_id = rng.gen_range(1..DIST_PER_WARE);
        let c_id = nu_rand(rng, 1023, 1, CUST_PER_DIST);
        let c_last = last_name(nu_rand(rng, 255, 0, 999));
        let h_amount = rng.gen_range(1..5000);
        let by_name = rng.gen_range(1..100) < 60;
        let (c_w_id, c_d_id) = if ALLOW_MULTI_WAREHOUSE_TX {
            if rng.gen_range(1..100) < 85 {
                (w_id, d_id)
            } else {
                (
                    other_ware(rng, w_id, num_ware),
                    rng.gen_range(1..DIST_PER_WARE),
                )
            }
        } else {
            (w_id, d_id)
        };
        let args = PaymentArgs::new(
            w_id,
            d_id,
            by_name,
            c_w_id,
            c_d_id,
            c_id,
            c_last,
            Decimal::from(h_amount),
        );
        Payment::run(tx, &args)?;

        Ok(())
    }
}

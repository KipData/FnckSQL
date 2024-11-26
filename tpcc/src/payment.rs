use crate::load::{last_name, nu_rand, CUST_PER_DIST, DIST_PER_WARE};
use crate::{other_ware, TpccArgs, TpccError, TpccTest, TpccTransaction, ALLOW_MULTI_WAREHOUSE_TX};
use chrono::Utc;
use fnck_sql::db::{DBTransaction, Statement};
use fnck_sql::storage::Storage;
use fnck_sql::types::value::DataValue;
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
    fn run(
        tx: &mut DBTransaction<S>,
        args: &Self::Args,
        statements: &[Statement],
    ) -> Result<(), TpccError> {
        let now = Utc::now();
        // "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?"
        let (_, tuples) = tx.execute(
            &statements[0],
            vec![
                ("?1", DataValue::Decimal(Some(args.h_amount))),
                ("?2", DataValue::Int16(Some(args.w_id as i16))),
            ],
        )?;
        // "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?"
        let (_, tuples) = tx.execute(
            &statements[1],
            vec![("?1", DataValue::Int16(Some(args.w_id as i16)))],
        )?;
        let w_street_1 = tuples[0].values[0].utf8().unwrap();
        let w_street_2 = tuples[0].values[1].utf8().unwrap();
        let w_city = tuples[0].values[2].utf8().unwrap();
        let w_state = tuples[0].values[3].utf8().unwrap();
        let w_zip = tuples[0].values[4].utf8().unwrap();
        let w_name = tuples[0].values[5].utf8().unwrap();

        // "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?"
        let (_, tuples) = tx.execute(
            &statements[2],
            vec![
                ("?1", DataValue::Decimal(Some(args.h_amount))),
                ("?2", DataValue::Int16(Some(args.w_id as i16))),
                ("?3", DataValue::Int8(Some(args.d_id as i8))),
            ],
        )?;

        // "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?"
        let (_, tuples) = tx.execute(
            &statements[3],
            vec![
                ("?1", DataValue::Int16(Some(args.w_id as i16))),
                ("?2", DataValue::Int8(Some(args.d_id as i8))),
            ],
        )?;
        let d_street_1 = tuples[0].values[0].utf8().unwrap();
        let d_street_2 = tuples[0].values[1].utf8().unwrap();
        let d_city = tuples[0].values[2].utf8().unwrap();
        let d_state = tuples[0].values[3].utf8().unwrap();
        let d_zip = tuples[0].values[4].utf8().unwrap();
        let d_name = tuples[0].values[5].utf8().unwrap();

        let mut c_id = args.c_id as i32;
        if args.by_name {
            // "SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?"
            let (_, tuples) = tx.execute(
                &statements[4],
                vec![
                    ("?1", DataValue::Int16(Some(args.c_w_id as i16))),
                    ("?2", DataValue::Int8(Some(args.c_d_id as i8))),
                    ("?3", DataValue::from(args.c_last.clone())),
                ],
            )?;
            let mut name_cnt = tuples[0].values[0].i32().unwrap();

            // "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first"
            let (_, tuples) = tx.execute(
                &statements[5],
                vec![
                    ("?1", DataValue::Int16(Some(args.c_w_id as i16))),
                    ("?2", DataValue::Int8(Some(args.c_d_id as i8))),
                    ("?3", DataValue::from(args.c_last.clone())),
                ],
            )?;
            if name_cnt % 2 == 1 {
                name_cnt += 1;
            }
            for n in 0..name_cnt / 2 {
                c_id = tuples[n as usize].values[0].i32().unwrap();
            }
        }
        // "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE"
        let (_, tuples) = tx.execute(
            &statements[6],
            vec![
                ("?1", DataValue::Int16(Some(args.c_w_id as i16))),
                ("?2", DataValue::Int8(Some(args.c_d_id as i8))),
                ("?3", DataValue::Int32(Some(c_id))),
            ],
        )?;
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
                let (_, tuples) = tx.execute(
                    &statements[7],
                    vec![
                        ("?1", DataValue::Int16(Some(args.c_w_id as i16))),
                        ("?2", DataValue::Int8(Some(args.c_d_id as i8))),
                        ("?3", DataValue::Int32(Some(c_id))),
                    ],
                )?;
                let c_data = tuples[0].values[0].utf8().unwrap();

                // https://github.com/AgilData/tpcc/blob/dfbabe1e35cc93b2bf2e107fc699eb29c2097e24/src/main/java/com/codefutures/tpcc/Payment.java#L284
                // let c_new_data = format!("| {} {} {} {} {} {} {}", c_id, args.c_d_id, args.c_w_id, args.d_id, args.w_id, args.h_amount, )

                // "UPDATE customer SET c_balance = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                let (_, tuples) = tx.execute(
                    &statements[8],
                    vec![
                        ("?1", DataValue::Decimal(Some(c_balance))),
                        ("?2", DataValue::from(c_data)),
                        ("?3", DataValue::Int16(Some(args.c_w_id as i16))),
                        ("?4", DataValue::Int8(Some(args.c_d_id as i8))),
                        ("?5", DataValue::Int32(Some(c_id))),
                    ],
                )?;
            } else {
                // "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                let (_, tuples) = tx.execute(
                    &statements[9],
                    vec![
                        ("?1", DataValue::Decimal(Some(c_balance))),
                        ("?2", DataValue::Int16(Some(args.c_w_id as i16))),
                        ("?3", DataValue::Int8(Some(args.c_d_id as i8))),
                        ("?4", DataValue::Int32(Some(c_id))),
                    ],
                )?;
            }
        } else {
            // "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
            let (_, tuples) = tx.execute(
                &statements[9],
                vec![
                    ("?1", DataValue::Decimal(Some(c_balance))),
                    ("?2", DataValue::Int16(Some(args.c_w_id as i16))),
                    ("?3", DataValue::Int8(Some(args.c_d_id as i8))),
                    ("?4", DataValue::Int32(Some(c_id))),
                ],
            )?;
        }
        let h_data = format!("\\0{d_name}    \\0");
        // "INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
        let (_, tuples) = tx.execute(
            &statements[10],
            vec![
                ("?1", DataValue::Int8(Some(args.c_d_id as i8))),
                ("?2", DataValue::Int16(Some(args.c_w_id as i16))),
                ("?3", DataValue::Int32(Some(c_id))),
                ("?4", DataValue::Int8(Some(args.d_id as i8))),
                ("?5", DataValue::Int16(Some(args.w_id as i16))),
                ("?6", DataValue::from(&now.naive_utc())),
                ("?7", DataValue::Decimal(Some(args.h_amount))),
                ("?8", DataValue::from(h_data)),
            ],
        )?;

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
        statements: &[Statement],
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
        Payment::run(tx, &args, statements)?;

        Ok(())
    }
}

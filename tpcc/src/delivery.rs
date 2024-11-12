use crate::load::DIST_PER_WARE;
use crate::{TpccArgs, TpccError, TpccTest, TpccTransaction};
use chrono::Utc;
use fnck_sql::db::DBTransaction;
use fnck_sql::storage::Storage;
use rand::prelude::ThreadRng;
use rand::Rng;

#[derive(Debug)]
pub(crate) struct DeliveryArgs {
    w_id: usize,
    o_carrier_id: usize,
}

impl DeliveryArgs {
    pub(crate) fn new(w_id: usize, o_carrier_id: usize) -> Self {
        Self { w_id, o_carrier_id }
    }
}

pub(crate) struct Delivery;
pub(crate) struct DeliveryTest;

impl<S: Storage> TpccTransaction<S> for Delivery {
    type Args = DeliveryArgs;

    fn run(tx: &mut DBTransaction<S>, args: &Self::Args) -> Result<(), TpccError> {
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        for d_id in 1..DIST_PER_WARE + 1 {
            // "SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = ? AND no_w_id = ?"
            let (_, tuple) = tx.run(format!("SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = {} AND no_w_id = {}", d_id, args.w_id))?;
            let no_o_id = tuple[0].values[0].i32().unwrap();

            if no_o_id == 0 {
                continue;
            }
            // "DELETE FROM new_orders WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?"
            let _ = tx.run(format!(
                "DELETE FROM new_orders WHERE no_o_id = {} AND no_d_id = {} AND no_w_id = {}",
                no_o_id, d_id, args.w_id
            ))?;
            // "SELECT o_c_id FROM orders WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?"
            let (_, tuple) = tx.run(format!(
                "SELECT o_c_id FROM orders WHERE o_id = {} AND o_d_id = {} AND o_w_id = {}",
                no_o_id, d_id, args.w_id
            ))?;
            let c_id = tuple[0].values[0].i32().unwrap();
            // "UPDATE orders SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?"
            let _ = tx.run(format!("UPDATE orders SET o_carrier_id = {} WHERE o_id = {} AND o_d_id = {} AND o_w_id = {}", args.o_carrier_id, no_o_id, d_id, args.w_id))?;
            // "UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?"
            let _ = tx.run(format!("UPDATE order_line SET ol_delivery_d = '{}' WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {}", now, no_o_id, d_id, args.w_id))?;
            // "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?"
            let (_, tuple) = tx.run(format!("SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {}", no_o_id, d_id, args.w_id))?;
            let ol_total = tuple[0].values[0].decimal().unwrap();
            // "UPDATE customer SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?"
            let _ = tx.run(format!("UPDATE customer SET c_balance = c_balance + {} , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = {} AND c_d_id = {} AND c_w_id = {}", ol_total, c_id, d_id, args.w_id))?;
        }

        Ok(())
    }
}

impl<S: Storage> TpccTest<S> for DeliveryTest {
    fn name(&self) -> &'static str {
        "Delivery"
    }

    fn do_transaction(
        &self,
        rng: &mut ThreadRng,
        tx: &mut DBTransaction<S>,
        num_ware: usize,
        _: &TpccArgs,
    ) -> Result<(), TpccError> {
        let w_id = rng.gen_range(0..num_ware) + 1;
        let o_carrier_id = rng.gen_range(1..10);

        let args = DeliveryArgs::new(w_id, o_carrier_id);
        Delivery::run(tx, &args)?;

        Ok(())
    }
}

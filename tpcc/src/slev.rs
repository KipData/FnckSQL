use crate::load::DIST_PER_WARE;
use crate::{TpccArgs, TpccError, TpccTest, TpccTransaction};
use fnck_sql::db::DBTransaction;
use fnck_sql::storage::Storage;
use rand::prelude::ThreadRng;
use rand::Rng;

#[derive(Debug)]
pub(crate) struct SlevArgs {
    w_id: usize,
    d_id: usize,
    level: usize,
}

impl SlevArgs {
    pub(crate) fn new(w_id: usize, d_id: usize, level: usize) -> Self {
        Self { w_id, d_id, level }
    }
}

pub(crate) struct Slev;
pub(crate) struct SlevTest;

impl<S: Storage> TpccTransaction<S> for Slev {
    type Args = SlevArgs;

    fn run(tx: &mut DBTransaction<S>, args: &Self::Args) -> Result<(), TpccError> {
        // "SELECT d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?"
        let (_, tuples) = tx.run(format!(
            "SELECT d_next_o_id FROM district WHERE d_id = {} AND d_w_id = {}",
            args.d_id, args.w_id
        ))?;
        let d_next_o_id = tuples[0].values[0].i32().unwrap();
        // "SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)"
        let (_, tuples) = tx.run(format!("SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id < {} AND ol_o_id >= ({} - 20)", args.w_id, args.d_id, d_next_o_id, d_next_o_id))?;
        let ol_i_id = tuples[0].values[0].i32().unwrap();
        // "SELECT count(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?"
        let (_, tuples) = tx.run(format!(
            "SELECT count(*) FROM stock WHERE s_w_id = {} AND s_i_id = {} AND s_quantity < {}",
            args.w_id, ol_i_id, args.level
        ))?;
        // let i_count = tuples[0].values[0].i32().unwrap();

        Ok(())
    }
}

impl<S: Storage> TpccTest<S> for SlevTest {
    fn name(&self) -> &'static str {
        "Stock-Level"
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
        let level = rng.gen_range(10..20);

        let args = SlevArgs::new(w_id, d_id, level);
        Slev::run(tx, &args)?;

        Ok(())
    }
}

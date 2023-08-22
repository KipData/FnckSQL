use comfy_table::{Cell, Table};
use itertools::Itertools;
use crate::catalog::ColumnCatalog;
use crate::types::value::DataValue;

pub type TupleId = usize;

#[derive(Clone, Debug, PartialEq)]
pub struct Tuple {
    pub id: Option<TupleId>,
    pub columns: Vec<ColumnCatalog>,
    pub values: Vec<DataValue>
}

pub fn create_table(tuples: &[Tuple]) -> Table {
    let mut table = Table::new();

    if tuples.is_empty() {
        return table;
    }

    let mut header = Vec::new();
    for col in &tuples[0].columns {
        header.push(Cell::new(col.name.clone()));
    }
    table.set_header(header);

    for tuple in tuples {
        let cells = tuple.values
            .iter()
            .map(|value| Cell::new(format!("{value}")))
            .collect_vec();

        table.add_row(cells);
    }

    table
}
pub mod hash_utils;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::util::display::array_value_to_string;

pub fn record_batch_to_string(batch: &RecordBatch) -> Result<String, ArrowError> {
    let mut output = String::new();
    for row in 0..batch.num_rows() {
        for col in 0..batch.num_columns() {
            if col != 0 {
                output.push(' ');
            }
            let column = batch.column(col);

            // NULL values are rendered as "NULL".
            if column.is_null(row) {
                output.push_str("NULL");
                continue;
            }
            let string = array_value_to_string(column, row)?;

            // Empty strings are rendered as "(empty)".
            if *column.data_type() == DataType::Utf8 && string.is_empty() {
                output.push_str("(empty)");
                continue;
            }
            output.push_str(&string);
        }
        output.push('\n');
    }

    Ok(output)
}
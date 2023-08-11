// copied from datafusion and deleted unused functions

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;

use crate::execution_v1::ExecutorError;

// Combines two hashes into one hash
#[inline]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

fn hash_null(random_state: &RandomState, hashes_buffer: &'_ mut [u64], mul_col: bool) {
    if mul_col {
        hashes_buffer.iter_mut().for_each(|hash| {
            // stable hash for null value
            *hash = combine_hashes(random_state.hash_one(&1), *hash);
        })
    } else {
        hashes_buffer.iter_mut().for_each(|hash| {
            *hash = random_state.hash_one(&1);
        })
    }
}

macro_rules! hash_array {
    (
        $array_type:ident,
        $column:ident,
        $ty:ty,
        $hashes:ident,
        $random_state:ident,
        $multi_col:ident
    ) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            if $multi_col {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = combine_hashes($random_state.hash_one(&array.value(i)), *hash);
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    *hash = $random_state.hash_one(&array.value(i));
                }
            }
        } else {
            if $multi_col {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = combine_hashes($random_state.hash_one(&array.value(i)), *hash);
                    }
                }
            } else {
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !array.is_null(i) {
                        *hash = $random_state.hash_one(&array.value(i));
                    }
                }
            }
        }
    };
}

macro_rules! hash_array_primitive {
    (
        $array_type:ident,
        $column:ident,
        $ty:ident,
        $hashes:ident,
        $random_state:ident,
        $multi_col:ident
    ) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            if $multi_col {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = combine_hashes($random_state.hash_one(value), *hash);
                }
            } else {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = $random_state.hash_one(value)
                }
            }
        } else {
            if $multi_col {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        *hash = combine_hashes($random_state.hash_one(value), *hash);
                    }
                }
            } else {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        *hash = $random_state.hash_one(value);
                    }
                }
            }
        }
    };
}

macro_rules! hash_array_float {
    (
        $array_type:ident,
        $column:ident,
        $ty:ident,
        $hashes:ident,
        $random_state:ident,
        $multi_col:ident
    ) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            if $multi_col {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = combine_hashes(
                        $random_state.hash_one(&$ty::from_le_bytes(value.to_le_bytes())),
                        *hash,
                    );
                }
            } else {
                for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                    *hash = $random_state.hash_one(&$ty::from_le_bytes(value.to_le_bytes()))
                }
            }
        } else {
            if $multi_col {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        *hash = combine_hashes(
                            $random_state.hash_one(&$ty::from_le_bytes(value.to_le_bytes())),
                            *hash,
                        );
                    }
                }
            } else {
                for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                    if !array.is_null(i) {
                        *hash = $random_state.hash_one(&$ty::from_le_bytes(value.to_le_bytes()));
                    }
                }
            }
        }
    };
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
#[cfg(not(feature = "force_hash_collisions"))]
pub fn create_hashes<'a>(
    arrays: &[ArrayRef],
    random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>, ExecutorError> {
    // combine hashes with `combine_hashes` if we have more than 1 column
    let multi_col = arrays.len() > 1;

    for col in arrays {
        match col.data_type() {
            DataType::Null => {
                hash_null(random_state, hashes_buffer, multi_col);
            }
            DataType::Int32 => {
                hash_array_primitive!(Int32Array, col, i32, hashes_buffer, random_state, multi_col);
            }
            DataType::Int64 => {
                hash_array_primitive!(Int64Array, col, i64, hashes_buffer, random_state, multi_col);
            }
            DataType::Float64 => {
                hash_array_float!(
                    Float64Array,
                    col,
                    u64,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Boolean => {
                hash_array!(
                    BooleanArray,
                    col,
                    u8,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            DataType::Utf8 => {
                hash_array!(
                    StringArray,
                    col,
                    str,
                    hashes_buffer,
                    random_state,
                    multi_col
                );
            }
            _ => {
                // This is internal because we should have caught this before.
                return Err(ExecutorError::InternalError(format!(
                    "Unsupported data type in hasher: {}",
                    col.data_type()
                )));
            }
        }
    }
    Ok(hashes_buffer)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn create_hashes_for_float_arrays() -> Result<(), ExecutorError> {
        let f64_arr = Arc::new(Float64Array::from_iter_values(vec![0.12, 0.5, 1f64, 444.7]));
        let f64_arr_2 = Arc::new(Float64Array::from_iter_values(vec![0.12, 0.5, 1f64, 444.7]));

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; f64_arr.len()];

        let hashes = create_hashes(&[f64_arr, f64_arr_2], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 4);
        assert_eq!(hashes.clone(), hashes_buff.clone());
        assert_eq!(
            hashes_buff,
            &[
                13192744372685867462,
                5527281222425499956,
                3851526787237496334,
                1092489821776418240,
            ]
        );
        Ok(())
    }
}

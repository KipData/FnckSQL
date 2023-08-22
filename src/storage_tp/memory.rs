use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::slice;
use std::sync::Arc;
use parking_lot::Mutex;
use crate::catalog::{ColumnCatalog, RootCatalog, TableCatalog};
use crate::storage_tp::{Bounds, Projections, Storage, StorageError, Table, Transaction};
use crate::types::TableId;
use crate::types::tuple::Tuple;

// WARRING: Only single-threaded and tested using
#[derive(Clone, Debug)]
pub struct MemStorage {
    inner: Arc<Mutex<StorageInner>>
}

impl MemStorage {
    pub fn new() -> MemStorage {
        Self {
            inner: Arc::new(
                Mutex::new(
                    StorageInner {
                        root: Default::default(),
                        tables: Default::default(),
                    }
                )
            ),
        }
    }
}

#[derive(Debug)]
struct StorageInner {
    root: RootCatalog,
    tables: HashMap<TableId, MemTable>
}

impl Storage for MemStorage {
    type TableType = MemTable;

    fn create_table(&self, table_name: String, columns: Vec<ColumnCatalog>) -> Result<TableId, StorageError> {
        let new_table = MemTable {
            tuples: Arc::new(Cell::new(vec![])),
        };

        let mut inner = self.inner.lock();

        let table_id = inner.root.add_table(table_name, columns)?;
        inner.tables.insert(table_id, new_table);

        Ok(table_id)
    }

    fn get_table(&self, id: &TableId) -> Result<Self::TableType, StorageError> {
        self.inner
            .lock()
            .tables
            .get(id)
            .cloned()
            .ok_or(StorageError::TableNotFound(*id))
    }

    fn get_catalog(&self) -> RootCatalog {
        self.inner
            .lock()
            .root
            .clone()
    }

    fn show_tables(&self) -> Result<Vec<TableCatalog>, StorageError> {
        todo!()
    }
}

unsafe impl Send for MemTable {

}

unsafe impl Sync for MemTable {

}

#[derive(Clone)]
pub struct MemTable {
    tuples: Arc<Cell<Vec<Tuple>>>
}

impl Debug for MemTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe {
            f.debug_struct("MemTable")
                .field("{:?}", self.tuples.as_ptr().as_ref().unwrap())
                .finish()
        }
    }
}

impl Table for MemTable {
    type TransactionType<'a> = MemTraction<'a>;

    fn read(&self, bounds: Bounds, projection: Projections) -> Result<Self::TransactionType<'_>, StorageError> {
        unsafe {
            Ok(
                MemTraction {
                    offset: bounds.0.unwrap_or(0),
                    limit: bounds.1,
                    projections: projection,
                    iter: self.tuples.as_ptr().as_ref().unwrap().iter(),
                }
            )
        }
    }

    fn append(&self, tuple: Tuple) -> Result<(), StorageError> {
        unsafe {
            self.tuples
                .as_ptr()
                .as_mut()
                .unwrap()
                .push(tuple);
        }

        Ok(())
    }
}

pub struct MemTraction<'a> {
    offset: usize,
    limit: Option<usize>,
    projections: Projections,
    iter: slice::Iter<'a, Tuple>
}

impl Transaction for MemTraction<'_> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError> {
        while self.offset > 0 {
            let _ = self.iter.next();
            self.offset -= 1;
        }

        if let Some(num) = self.limit {
            if num == 0 {
                return Ok(None);
            }
        }

        Ok(self.iter
            .next()
            .cloned()
            .map(|tuple| {
                let projection_len = self.projections.len();

                let mut columns = Vec::with_capacity(projection_len);
                let mut values = Vec::with_capacity(projection_len);

                for expr in self.projections.iter() {
                    values.push(expr.eval_column_tp(&tuple));
                    columns.push(expr.output_column(&tuple));
                }

                self.limit = self.limit.map(|num| num - 1);

                Tuple {
                    id: tuple.id,
                    columns,
                    values,
                }
            }))
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::ScalarExpression;
    use crate::storage_tp::memory::{MemStorage, MemTable};
    use crate::storage_tp::{Storage, StorageError, Table, Transaction};
    use crate::types::LogicalType;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;

    fn data_filling(columns: Vec<ColumnCatalog>, table: &MemTable) -> Result<(), StorageError> {
        table.append(Tuple {
            id: Some(0),
            columns: columns.clone(),
            values: vec![
                DataValue::Int32(Some(1)),
                DataValue::Boolean(Some(true))
            ],
        })?;
        table.append(Tuple {
            id: Some(1),
            columns: columns.clone(),
            values: vec![
                DataValue::Int32(Some(2)),
                DataValue::Boolean(Some(false))
            ],
        })?;

        Ok(())
    }

    #[test]
    fn test_in_memory_storage_works_with_data() -> Result<(), StorageError> {
        let storage = MemStorage::new();
        let columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true)
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false)
            ),
        ];

        let table_id = storage.create_table("test".to_string(), columns.clone())?;

        let catalog = storage.get_catalog();
        let table_catalog = catalog.get_table_by_name("test");
        assert!(table_catalog.is_some());
        assert!(table_catalog.unwrap().get_column_id_by_name("c1").is_some());

        let table = storage.get_table(&table_id)?;
        data_filling(columns, &table)?;

        let mut tx = table.read(
            (Some(1), Some(1)),
            vec![ScalarExpression::InputRef { index: 0, ty: LogicalType::Integer }]
        )?;

        let option_1 = tx.next_tuple()?;
        assert_eq!(option_1.unwrap().id, Some(1));

        let option_2 = tx.next_tuple()?;
        assert_eq!(option_2, None);

        Ok(())
    }
}
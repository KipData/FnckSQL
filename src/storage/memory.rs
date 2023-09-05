use std::cell::Cell;
use std::fmt::{Debug, Formatter};
use std::slice;
use std::sync::Arc;
use itertools::Itertools;
use crate::catalog::{ColumnCatalog, ColumnRef, RootCatalog, TableCatalog, TableName};
use crate::storage::{Bounds, Projections, Storage, StorageError, Table, Transaction};
use crate::types::tuple::Tuple;

// WARRING: Only single-threaded and tested using
#[derive(Clone)]
pub struct MemStorage {
    inner: Arc<Cell<StorageInner>>
}

unsafe impl Send for MemStorage {

}

unsafe impl Sync for MemStorage {

}

impl MemStorage {
    pub fn new() -> MemStorage {
        Self {
            inner: Arc::new(
                Cell::new(
                    StorageInner {
                        root: Default::default(),
                        tables: Default::default(),
                    }
                )
            ),
        }
    }

    pub fn root(self, root: RootCatalog) -> Self {
        unsafe {
            self.inner.as_ptr().as_mut().unwrap().root = root;
        }
        self
    }
}

#[derive(Debug)]
struct StorageInner {
    root: RootCatalog,
    tables: Vec<(TableName, MemTable)>
}

impl Storage for MemStorage {
    type TableType = MemTable;

    fn create_table(&self, table_name: TableName, columns: Vec<ColumnRef>) -> Result<TableName, StorageError> {
        let new_table = MemTable {
            tuples: Arc::new(Cell::new(vec![])),
        };
        let de_columns = columns.iter()
            .map(|col| ColumnCatalog::clone(col))
            .collect_vec();

        let inner = unsafe { self.inner.as_ptr().as_mut() }.unwrap();

        let table_id = inner.root.add_table(table_name.clone(), de_columns)?;
        inner.tables.push((table_name, new_table));

        Ok(table_id)
    }

    fn table(&self, name: &String) -> Option<Self::TableType> {
        unsafe {
            self.inner
                .as_ptr()
                .as_ref()
                .unwrap()
                .tables
                .iter()
                .find(|(tname, _)| tname.as_str() == name)
                .map(|(_, table)| table.clone())
        }
    }

    fn table_catalog(&self, name: &String) -> Option<&TableCatalog> {
        unsafe {
            self.inner
                .as_ptr()
                .as_ref()
                .unwrap()
                .root
                .get_table(name)
        }
    }

    fn tables(&self) -> Vec<&TableCatalog> {
        unsafe {
            self.inner
                .as_ptr()
                .as_ref()
                .unwrap()
                .root
                .tables()
        }
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

    fn append(&mut self, tuple: Tuple) -> Result<(), StorageError> {
        let tuples = unsafe {
            self.tuples
                .as_ptr()
                .as_mut()
        }.unwrap();

        if let Some(original_tuple) = tuples.iter_mut().find(|t| t.id == tuple.id) {
            *original_tuple = tuple;
        } else {
            tuples.push(tuple);
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
                    values.push(expr.eval_column(&tuple));
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
    use std::sync::Arc;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::expression::ScalarExpression;
    use crate::storage::memory::{MemStorage, MemTable};
    use crate::storage::{Storage, StorageError, Table, Transaction};
    use crate::types::LogicalType;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;

    fn data_filling(columns: Vec<ColumnRef>, table: &mut MemTable) -> Result<(), StorageError> {
        table.append(Tuple {
            id: Some(0),
            columns: columns.clone(),
            values: vec![
                Arc::new(DataValue::Int32(Some(1))),
                Arc::new(DataValue::Boolean(Some(true)))
            ],
        })?;
        table.append(Tuple {
            id: Some(1),
            columns: columns.clone(),
            values: vec![
                Arc::new(DataValue::Int32(Some(2))),
                Arc::new(DataValue::Boolean(Some(false)))
            ],
        })?;

        Ok(())
    }

    #[test]
    fn test_in_memory_storage_works_with_data() -> Result<(), StorageError> {
        let storage = MemStorage::new();
        let columns = vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true)
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false)
            )),
        ];

        let table_id = storage.create_table(Arc::new("test".to_string()), columns.clone())?;

        let table_catalog = storage.table_catalog(&"test".to_string());
        assert!(table_catalog.is_some());
        assert!(table_catalog.unwrap().get_column_id_by_name(&"c1".to_string()).is_some());

        let mut table = storage.table(&table_id).unwrap();
        data_filling(columns, &mut table)?;

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
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

    fn read(&self, _bounds: Bounds, _projection: Projections) -> Result<Self::TransactionType<'_>, StorageError> {
        unsafe {
            Ok(
                MemTraction {
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
    iter: slice::Iter<'a, Tuple>
}

impl Transaction for MemTraction<'_> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError> {
        Ok(self.iter.next().cloned())
    }
}
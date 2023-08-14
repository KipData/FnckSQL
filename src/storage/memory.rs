use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog};
use crate::storage::{Bounds, Projections, Storage, StorageError, Table, Transaction};
use crate::types::{LogicalType, TableId};

#[derive(Debug)]
pub struct InMemoryStorage {
    inner: Arc<Mutex<StorageInner>>,
}

#[derive(Debug)]
struct StorageInner {
    catalog: RootCatalog,
    tables: BTreeMap<TableId, InMemoryTable>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            inner: Arc::new(Mutex::new(
                StorageInner {
                    catalog: RootCatalog::default(),
                    tables: BTreeMap::new(),
                })
            )
        }
    }
}

impl Clone for InMemoryStorage {
    fn clone(&self) -> Self {
        InMemoryStorage {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Storage for InMemoryStorage {
    type TableType = InMemoryTable;

    fn create_table(
        &self,
        table_name: &str,
        data: Vec<RecordBatch>,
    ) -> Result<TableId, StorageError> {
        let mut table = InMemoryTable::new(table_name, data)?;
        let mut inner = self.inner.lock();

        let table_id = inner.catalog.add_table(
            table_name.to_string(),
            table.inner.lock().columns.clone()
        )?;

        table.table_id = table_id;
        inner.tables.insert(table_id, table);

        Ok(table_id)
    }

    fn get_table(&self, id: &TableId) -> Result<Self::TableType, StorageError> {
        self.inner.lock()
            .tables
            .get(id)
            .cloned()
            .ok_or(StorageError::TableNotFound(*id))
    }

    fn get_catalog(&self) -> RootCatalog {
        self.inner.lock()
            .catalog.clone()
    }

    fn show_tables(&self) -> Result<RecordBatch, StorageError> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryTable {
    table_id: TableId,
    table_name: String,
    inner: Arc<Mutex<TableInner>>
}

#[derive(Debug)]
struct TableInner {
    data: Vec<RecordBatch>,
    columns: Vec<ColumnCatalog>,
}

impl InMemoryTable {
    pub fn new(name: &str, data: Vec<RecordBatch>) -> Result<Self, StorageError> {
        let columns = Self::infer_catalog(data.first().cloned());
        Ok(Self {
            table_id: 0,
            table_name: name.to_string(),

            inner: Arc::new(Mutex::new(
                TableInner {
                    data,
                    columns,
                }
            )),
        })
    }

    fn infer_catalog(batch: Option<RecordBatch>) -> Vec<ColumnCatalog> {
        let mut columns = Vec::new();
        if let Some(batch) = batch {
            for f in batch.schema().fields().iter() {
                let field_name = f.name().to_string();
                let column_desc =
                    ColumnDesc::new(LogicalType::try_from(f.data_type()).unwrap(), false);
                let column_catalog = ColumnCatalog::new(
                    field_name,
                    f.is_nullable(),
                    column_desc
                );
                columns.push(column_catalog)
            }
        }
        columns
    }
}

impl Table for InMemoryTable {
    type TransactionType = InMemoryTransaction;

    fn read(
        &self,
        _bounds: Bounds,
        _projection: Projections,
    ) -> Result<Self::TransactionType, StorageError> {
        InMemoryTransaction::start(self)
    }

    fn append(&self, record_batch: RecordBatch) -> Result<(), StorageError> {
        self.inner.lock()
            .data.push(record_batch);

        Ok(())
    }
}

pub struct InMemoryTransaction {
    batch_cursor: usize,
    data: Vec<RecordBatch>,
}

impl InMemoryTransaction {
    pub fn start(table: &InMemoryTable) -> Result<Self, StorageError> {
        Ok(Self {
            batch_cursor: 0,
            data: table.inner.lock().data.clone(),
        })
    }
}

impl Transaction for InMemoryTransaction {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, StorageError> {
        self.data
            .get(self.batch_cursor)
            .map(|batch| {
                self.batch_cursor += 1;
                Ok(batch.clone())
            })
            .transpose()
    }
}

#[cfg(test)]
mod storage_test {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn build_record_batch() -> Result<Vec<RecordBatch>, StorageError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )?;
        Ok(vec![batch])
    }

    #[test]
    fn test_in_memory_storage_works_with_data() -> Result<(), StorageError> {
        let mut storage = InMemoryStorage::new();

        let id = storage.create_table("test", build_record_batch()?)?;
        let catalog = storage.get_catalog();
        println!("{:?}", catalog);
        let table_catalog = catalog.get_table_by_name("test");
        assert!(table_catalog.is_some());
        assert!(table_catalog.unwrap().get_column_id_by_name("a").is_some());

        let table = storage.get_table(&id)?;
        let mut tx = table.read(None, None)?;
        let batch = tx.next_batch()?;
        println!("{:?}", batch);
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().num_rows(), 3);

        Ok(())
    }
}

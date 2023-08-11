use std::collections::BTreeMap;
use std::sync::Arc;
use arrow::datatypes::{Schema, SchemaRef};

use arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog};
use crate::expression::ScalarExpression;
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
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::TransactionType, StorageError> {
        InMemoryTransaction::start(self, bounds, projection)
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
    pub fn start(table: &InMemoryTable, bounds: Bounds, projection: Projections) -> Result<Self, StorageError> {
        let inner = table.inner.lock();

        let (offset, limit) = bounds;
        let offset_val = offset.unwrap_or(0);
        if limit.is_some() && limit.unwrap() == 0 {
            return Ok(Self {
                batch_cursor: 0,
                data: inner.data.clone(),
            })
        }

        let mut returned_count = 0;
        let mut batches = Vec::new();

        for batch in &inner.data {
            let cardinality = batch.num_rows();
            let limit_val = limit.unwrap_or(cardinality);

            let start = returned_count.max(offset_val) - returned_count;
            let end = {
                // from total returned rows level, the total_end is end index of whole returned
                // rows level.
                let total_end = offset_val + limit_val;
                let current_batch_end = returned_count + cardinality;
                // we choose the min of total_end and current_batch_end as the end index of to
                // match limit semantics.
                let real_end = total_end.min(current_batch_end);
                // to calculate the end index of current batch
                real_end - returned_count
            };
            returned_count += cardinality;

            // example: offset=1000, limit=2, cardinality=100
            // when first loop:
            // start = 0.max(1000)-0 = 1000
            // end = (1000+2).min(0+100)-0 = 100
            // so, start(1000) > end(100), we skip this loop batch.
            if start >= end {
                continue;
            }

            if (start..end) == (0..cardinality) {
                batches.push(projection_batch(&projection, batch.clone())?);
            } else {
                let length = end - start;
                batches.push(projection_batch(&projection, batch.slice(start, length))?);
            }

            // dut to returned_count is always += cardinality, and returned_batch maybe slsliced,
            // so it will larger than real total_end.
            // example: offset=1, limit=4, cardinality=6, data=[(0..6)]
            // returned_count=6 > 1+4, meanwhile returned_batch size is 4 ([0..5])
            if returned_count >= offset_val + limit_val {
                break;
            }
        }

        Ok(Self {
            batch_cursor: 0,
            data: batches,
        })
    }
}

fn projection_batch(exprs: &Vec<ScalarExpression>, batch: RecordBatch) -> Result<RecordBatch, StorageError> {
    if exprs.is_empty() {
        Ok(batch)
    } else {
        let columns = exprs
            .iter()
            .map(|e| e.eval_column(&batch))
            .try_collect()
            .unwrap();
        let fields = exprs.iter().map(|e| e.eval_field(&batch)).collect();
        let schema = SchemaRef::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        ));
        Ok(RecordBatch::try_new(schema, columns).unwrap())
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

    use arrow::array::{Array, Int32Array};
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
        let mut tx = table.read(
            (Some(0), Some(1)),
            vec![ScalarExpression::InputRef { index: 0, ty: LogicalType::Integer }]
        )?;
        let batch = tx.next_batch()?.unwrap();
        println!("{:?}", batch);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        assert_eq!(
            batch.columns()[0].data(),
            Arc::new(Int32Array::from(vec![1])).data()
        );

        Ok(())
    }
}

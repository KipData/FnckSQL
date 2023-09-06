use core::slice::SlicePattern;
use std::collections::Bound;
use std::sync::Arc;
use async_trait::async_trait;
use kip_db::kernel::lsm::mvcc::TransactionIter;
use kip_db::kernel::lsm::{mvcc, storage};
use kip_db::kernel::lsm::iterator::Iter;
use kip_db::kernel::Storage as Kip_Storage;
use kip_db::kernel::utils::lru_cache::ShardingLruCache;
use crate::catalog::{ColumnCatalog, TableCatalog, TableName};
use crate::storage::{Bounds, Projections, Storage, StorageError, Table, Transaction};
use crate::storage::table_codec::TableCodec;
use crate::types::tuple::Tuple;

#[derive(Clone)]
pub struct KipStorage {
    cache: Arc<ShardingLruCache<TableName, TableCatalog>>,
    inner: Arc<storage::KipStorage>
}

#[async_trait]
impl Storage for KipStorage {
    type TableType = KipTable;

    async fn create_table(&self, table_name: TableName, columns: Vec<ColumnCatalog>) -> Result<TableName, StorageError> {
        let table = TableCatalog::new(table_name.clone(), columns)?;
        for (key, value) in table.columns
            .iter()
            .filter_map(|(_, col)| TableCodec::encode_column(col))
        {
            self.inner.set(key.as_slice(), value).await?;
        }

        self.cache.put(table_name.clone(), table);

        Ok(table_name)
    }

    async fn table(&self, name: &String) -> Option<Self::TableType> {
        todo!()
    }

    async fn table_catalog(&self, name: &String) -> Option<&TableCatalog> {
        todo!()
    }
}

pub struct KipTable {
    table_codec: TableCodec,
    tx: mvcc::Transaction
}

impl Table for KipTable {
    type TransactionType<'a> = KipTraction<'a>;

    fn read(&self, bounds: Bounds, projections: Projections) -> Result<Self::TransactionType<'_>, StorageError> {
        let (min, max) = self.table_codec.tuple_bound();
        let iter = self.tx.iter(Bound::Included(&min), Bound::Included(&max))?;

        Ok(KipTraction {
            offset: bounds.0.unwrap_or(0),
            limit: bounds.1,
            projections,
            table_codec: &self.table_codec,
            iter,
        })
    }

    fn append(&mut self, tuple: Tuple) -> Result<(), StorageError> {
        let (key, value) = self.table_codec.encode_tuple(&tuple);
        self.tx.set(key.as_slice(), value);

        Ok(())
    }
}

pub struct KipTraction<'a> {
    offset: usize,
    limit: Option<usize>,
    projections: Projections,
    table_codec: &'a TableCodec,
    iter: TransactionIter<'a>
}

impl Transaction for KipTraction<'_> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError> {
        while self.offset > 0 {
            let _ = self.iter.try_next()?;
            self.offset -= 1;
        }

        if let Some(num) = self.limit {
            if num == 0 {
                return Ok(None);
            }
        }

        Ok(self.iter
            .try_next()?
            .and_then(|(key, bytes)| {
                bytes.and_then(|value| {
                    self.table_codec.decode_tuple(&key, &value)
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
                        })
                })
            }))
    }
}
use std::collections::Bound;
use std::collections::hash_map::RandomState;
use std::path::PathBuf;
use std::sync::Arc;
use async_trait::async_trait;
use kip_db::kernel::lsm::mvcc::TransactionIter;
use kip_db::kernel::lsm::{mvcc, storage};
use kip_db::kernel::lsm::iterator::Iter;
use kip_db::kernel::lsm::storage::Config;
use kip_db::kernel::Storage as Kip_Storage;
use kip_db::kernel::utils::lru_cache::ShardingLruCache;
use crate::catalog::{ColumnCatalog, TableCatalog, TableName};
use crate::storage::{Bounds, Projections, Storage, StorageError, Table, Transaction};
use crate::storage::table_codec::TableCodec;
use crate::types::tuple::{Tuple, TupleId};

#[derive(Clone)]
pub struct KipStorage {
    cache: Arc<ShardingLruCache<String, TableCatalog>>,
    pub inner: Arc<storage::KipStorage>
}

impl KipStorage {
    pub async fn new(path: impl Into<PathBuf> + Send) -> Result<Self, StorageError> {
        let config = Config::new(path);
        let storage = storage::KipStorage::open_with_config(config).await?;

        Ok(KipStorage {
            cache: Arc::new(ShardingLruCache::new(
                128,
                16,
                RandomState::default(),
            )?),
            inner: Arc::new(storage),
        })
    }
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
            self.inner.set(key, value).await?;
        }
        self.cache.put(table_name.to_string(), table);

        Ok(table_name)
    }

    async fn drop_table(&self, name: &String) -> Result<(), StorageError> {
        self.drop_data(name).await?;

        let (min, max) = TableCodec::columns_bound(name);
        let mut tx = self.inner.new_transaction().await;
        let mut iter = tx.iter(Bound::Included(&min), Bound::Included(&max))?;
        let mut col_keys = vec![];

        while let Some((key, value_option))  = iter.try_next()? {
            if value_option.is_some() {
                col_keys.push(key);
            }
        }
        drop(iter);

        for col_key in col_keys {
            tx.remove(&col_key)?
        }
        tx.commit().await?;

        let _ = self.cache.remove(name);

        Ok(())
    }

    async fn drop_data(&self, name: &String) -> Result<(), StorageError> {
        if let Some(mut table) = self.table(name).await {
            let (min, max) = table.table_codec.tuple_bound();
            let mut iter = table.tx.iter(Bound::Included(&min), Bound::Included(&max))?;
            let mut data_keys = vec![];

            while let Some((key, value_option))  = iter.try_next()? {
                if value_option.is_some() {
                    data_keys.push(key);
                }
            }
            drop(iter);

            for col_key in data_keys {
                table.tx.remove(&col_key)?
            }
            table.tx.commit().await?;
        }

        Ok(())
    }

    async fn table(&self, name: &String) -> Option<Self::TableType> {
        let table_codec = self.table_catalog(name)
            .await
            .map(|catalog| TableCodec { table: catalog.clone() })?;
        let tx = self.inner.new_transaction().await;

        Some(KipTable { table_codec, tx, })
    }

    async fn table_catalog(&self, name: &String) -> Option<&TableCatalog> {
        let mut option = self.cache.get(name);

        if option.is_none() {
            let (min, max) = TableCodec::columns_bound(name);
            let tx = self.inner.new_transaction().await;
            let mut iter = tx.iter(Bound::Included(&min), Bound::Included(&max)).ok()?;

            let mut columns = vec![];
            let mut name_option = None;

            while let Some((key, value_option))  = iter.try_next().ok().flatten() {
                if let Some(value) = value_option {
                    if let Some((table_name, column)) = TableCodec::decode_column(&key, &value) {
                        if name != table_name.as_str() { return None; }
                        let _ = name_option.insert(table_name);

                        columns.push(column);
                    }
                }
            }

            if let Some(catalog) = name_option.and_then(|table_name| TableCatalog::new(table_name, columns).ok()) {
                option = self.cache.get_or_insert(name.to_string(), |_| Ok(catalog)).ok();
            }
        }

        option
    }
}

pub struct KipTable {
    table_codec: TableCodec,
    tx: mvcc::Transaction
}

#[async_trait]
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
        self.tx.set(key, value);

        Ok(())
    }

    fn delete(&mut self, tuple_id: TupleId) -> Result<(), StorageError> {
        let key = self.table_codec.encode_tuple_key(&tuple_id);
        self.tx.remove(&key)?;

        Ok(())
    }

    async fn commit(self) -> Result<(), StorageError> {
        self.tx.commit().await?;

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

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::ScalarExpression;
    use crate::storage::kip::KipStorage;
    use crate::storage::{Storage, StorageError, Transaction, Table};
    use crate::storage::memory::test::data_filling;
    use crate::types::LogicalType;

    #[tokio::test]
    async fn test_in_kipdb_storage_works_with_data() -> Result<(), StorageError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
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

        let source_columns = columns.iter()
            .map(|col_ref| ColumnCatalog::clone(&col_ref))
            .collect_vec();
        let table_id = storage.create_table(Arc::new("test".to_string()), source_columns).await?;

        let table_catalog = storage.table_catalog(&"test".to_string()).await;
        assert!(table_catalog.is_some());
        assert!(table_catalog.unwrap().get_column_id_by_name(&"c1".to_string()).is_some());

        let mut table = storage.table(&table_id).await.unwrap();
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
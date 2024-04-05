use crate::catalog::TableCatalog;
use crate::errors::DatabaseError;
use crate::storage::{InnerIter, StatisticsMetaCache, Storage, Transaction};
use bytes::Bytes;
use kip_db::kernel::lsm::iterator::Iter as KipDBIter;
use kip_db::kernel::lsm::mvcc::{CheckType, TransactionIter};
use kip_db::kernel::lsm::storage::Config;
use kip_db::kernel::lsm::{mvcc, storage};
use kip_db::kernel::utils::lru_cache::ShardingLruCache;
use std::collections::hash_map::RandomState;
use std::collections::Bound;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct KipStorage {
    pub inner: Arc<storage::KipStorage>,
    pub(crate) meta_cache: Arc<StatisticsMetaCache>,
    pub(crate) table_cache: Arc<ShardingLruCache<String, TableCatalog>>,
}

impl KipStorage {
    pub async fn new(path: impl Into<PathBuf> + Send) -> Result<Self, DatabaseError> {
        let storage =
            storage::KipStorage::open_with_config(Config::new(path).enable_level_0_memorization())
                .await?;
        let meta_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new()).unwrap());
        let table_cache = Arc::new(ShardingLruCache::new(128, 16, RandomState::new()).unwrap());

        Ok(KipStorage {
            inner: Arc::new(storage),
            meta_cache,
            table_cache,
        })
    }
}

impl Storage for KipStorage {
    type TransactionType = KipTransaction;

    async fn transaction(&self) -> Result<Self::TransactionType, DatabaseError> {
        let tx = self.inner.new_transaction(CheckType::Optimistic).await;

        Ok(KipTransaction {
            tx,
            table_cache: Arc::clone(&self.table_cache),
            meta_cache: self.meta_cache.clone(),
        })
    }
}

pub struct KipTransaction {
    tx: mvcc::Transaction,
    table_cache: Arc<ShardingLruCache<String, TableCatalog>>,
    meta_cache: Arc<StatisticsMetaCache>,
}

impl Transaction for KipTransaction {
    type IterType<'a> = KipIter<'a>;

    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, DatabaseError> {
        Ok(self.tx.get(key)?)
    }

    fn range<'a>(
        &'a self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
    ) -> Result<Self::IterType<'a>, DatabaseError> {
        Ok(KipIter {
            iter: self.tx.iter(min, max)?,
        })
    }

    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), DatabaseError> {
        self.tx.set(key, value);

        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        self.tx.remove(key)?;

        Ok(())
    }

    fn table_cache(&self) -> &ShardingLruCache<String, TableCatalog> {
        self.table_cache.as_ref()
    }

    fn meta_cache(&self) -> &StatisticsMetaCache {
        self.meta_cache.as_ref()
    }

    async fn commit(self) -> Result<(), DatabaseError> {
        self.tx.commit().await?;

        Ok(())
    }
}

pub struct KipIter<'a> {
    iter: TransactionIter<'a>,
}

impl InnerIter for KipIter<'_> {
    fn try_next(&mut self) -> Result<Option<(Bytes, Bytes)>, DatabaseError> {
        while let Some((key, value_option)) = self.iter.try_next()? {
            if let Some(value) = value_option {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::DataBaseBuilder;
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::storage::kipdb::KipStorage;
    use crate::storage::{
        IndexImplEnum, IndexImplParams, IndexIter, Iter, PrimaryKeyIndexImpl, Storage, Transaction,
    };
    use crate::types::index::{IndexMeta, IndexType};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use itertools::Itertools;
    use std::collections::{Bound, VecDeque};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_in_kipdb_storage_works_with_data() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let mut transaction = storage.transaction().await?;
        let columns = Arc::new(vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false, None),
            )),
        ]);

        let source_columns = columns
            .iter()
            .map(|col_ref| ColumnCatalog::clone(&col_ref))
            .collect_vec();
        let _ = transaction.create_table(Arc::new("test".to_string()), source_columns, false)?;

        let table_catalog = transaction.table(Arc::new("test".to_string()));
        assert!(table_catalog.is_some());
        assert!(table_catalog
            .unwrap()
            .get_column_id_by_name(&"c1".to_string())
            .is_some());

        transaction.append(
            &"test".to_string(),
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(1)))),
                values: vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Boolean(Some(true))),
                ],
            },
            &[LogicalType::Integer, LogicalType::Boolean],
            false,
        )?;
        transaction.append(
            &"test".to_string(),
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(2)))),
                values: vec![
                    Arc::new(DataValue::Int32(Some(2))),
                    Arc::new(DataValue::Boolean(Some(false))),
                ],
            },
            &[LogicalType::Integer, LogicalType::Boolean],
            false,
        )?;

        let mut iter = transaction.read(
            Arc::new("test".to_string()),
            (Some(1), Some(1)),
            vec![(0, columns[0].clone())],
        )?;

        let option_1 = iter.next_tuple()?;
        assert_eq!(
            option_1.unwrap().id,
            Some(Arc::new(DataValue::Int32(Some(2))))
        );

        let option_2 = iter.next_tuple()?;
        assert_eq!(option_2, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_index_iter_pk() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build().await?;

        let _ = fnck_sql.run("create table t1 (a int primary key)").await?;
        let _ = fnck_sql
            .run("insert into t1 (a) values (0), (1), (2), (3), (4)")
            .await?;
        let transaction = fnck_sql.storage.transaction().await?;

        let table_name = Arc::new("t1".to_string());
        let table = transaction.table(table_name.clone()).unwrap().clone();
        let tuple_ids = vec![
            Arc::new(DataValue::Int32(Some(0))),
            Arc::new(DataValue::Int32(Some(2))),
            Arc::new(DataValue::Int32(Some(3))),
            Arc::new(DataValue::Int32(Some(4))),
        ];
        let mut iter = IndexIter {
            offset: 0,
            limit: None,
            params: IndexImplParams {
                tuple_schema_ref: table.schema_ref().clone(),
                projections: vec![0],
                index_meta: Arc::new(IndexMeta {
                    id: 0,
                    column_ids: vec![0],
                    table_name,
                    pk_ty: LogicalType::Integer,
                    name: "pk_a".to_string(),
                    ty: IndexType::PrimaryKey,
                }),
                table_name: &table.name,
                table_types: table.types(),
                tx: &transaction,
            },
            ranges: VecDeque::from(vec![
                Range::Eq(Arc::new(DataValue::Int32(Some(0)))),
                Range::Scope {
                    min: Bound::Included(Arc::new(DataValue::Int32(Some(2)))),
                    max: Bound::Included(Arc::new(DataValue::Int32(Some(4)))),
                },
            ]),
            scope_iter: None,
            inner: IndexImplEnum::PrimaryKey(PrimaryKeyIndexImpl),
        };
        let mut result = Vec::new();

        while let Some(tuple) = iter.next_tuple()? {
            result.push(tuple.id.unwrap());
        }

        assert_eq!(result, tuple_ids);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_by_index() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build().await?;
        let _ = fnck_sql
            .run("create table t1 (a int primary key, b int unique)")
            .await?;
        let _ = fnck_sql
            .run("insert into t1 (a, b) values (0, 0), (1, 1), (2, 2)")
            .await?;
        let transaction = fnck_sql.storage.transaction().await.unwrap();

        let table = transaction
            .table(Arc::new("t1".to_string()))
            .unwrap()
            .clone();
        let columns = table.columns().cloned().enumerate().collect_vec();
        let mut iter = transaction
            .read_by_index(
                Arc::new("t1".to_string()),
                (Some(0), Some(1)),
                columns,
                table.indexes[0].clone(),
                vec![Range::Scope {
                    min: Bound::Excluded(Arc::new(DataValue::Int32(Some(0)))),
                    max: Bound::Unbounded,
                }],
            )
            .unwrap();

        while let Some(tuple) = iter.next_tuple()? {
            assert_eq!(tuple.id, Some(Arc::new(DataValue::Int32(Some(1)))));
            assert_eq!(
                tuple.values,
                vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Int32(Some(1)))
                ]
            )
        }

        Ok(())
    }
}

use crate::errors::DatabaseError;
use crate::storage::{InnerIter, Storage, Transaction};
use bytes::Bytes;
use rocksdb::{DBIteratorWithThreadMode, Direction, IteratorMode, OptimisticTransactionDB};
use std::collections::Bound;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct RocksStorage {
    pub inner: Arc<OptimisticTransactionDB>,
}

impl RocksStorage {
    pub fn new(path: impl Into<PathBuf> + Send) -> Result<Self, DatabaseError> {
        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&rocksdb::Cache::new_lru_cache(40 * 1_024 * 1_024));

        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&bb);
        opts.create_if_missing(true);

        let storage = OptimisticTransactionDB::open(&opts, path.into())?;

        Ok(RocksStorage {
            inner: Arc::new(storage),
        })
    }
}

impl Storage for RocksStorage {
    type TransactionType<'a>
        = RocksTransaction<'a>
    where
        Self: 'a;

    fn transaction(&self) -> Result<Self::TransactionType<'_>, DatabaseError> {
        Ok(RocksTransaction {
            tx: self.inner.transaction(),
        })
    }
}

pub struct RocksTransaction<'db> {
    tx: rocksdb::Transaction<'db, OptimisticTransactionDB>,
}

impl<'txn> Transaction for RocksTransaction<'txn> {
    type IterType<'iter>
        = RocksIter<'txn, 'iter>
    where
        Self: 'iter;

    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, DatabaseError> {
        Ok(self.tx.get(key)?.map(Bytes::from))
    }

    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), DatabaseError> {
        self.tx.put(key, value)?;

        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        self.tx.delete(key)?;

        Ok(())
    }

    // Tips: rocksdb has weak support for `Include` and `Exclude`, so precision will be lost
    fn range<'a>(
        &'a self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
    ) -> Result<Self::IterType<'a>, DatabaseError> {
        fn bound_to_include(bound: Bound<&[u8]>) -> Option<&[u8]> {
            match bound {
                Bound::Included(bytes) | Bound::Excluded(bytes) => Some(bytes),
                Bound::Unbounded => None,
            }
        }

        let lower = bound_to_include(min)
            .map(|bytes| IteratorMode::From(bytes, Direction::Forward))
            .unwrap_or(IteratorMode::Start);
        let iter = self.tx.iterator(lower);

        Ok(RocksIter {
            lower: min.map(|bytes| bytes.to_vec()),
            upper: max.map(|bytes| bytes.to_vec()),
            iter,
        })
    }

    fn commit(self) -> Result<(), DatabaseError> {
        self.tx.commit()?;
        Ok(())
    }
}

pub struct RocksIter<'txn, 'iter> {
    lower: Bound<Vec<u8>>,
    upper: Bound<Vec<u8>>,
    iter: DBIteratorWithThreadMode<'iter, rocksdb::Transaction<'txn, OptimisticTransactionDB>>,
}

impl InnerIter for RocksIter<'_, '_> {
    fn try_next(&mut self) -> Result<Option<(Bytes, Bytes)>, DatabaseError> {
        for result in self.iter.by_ref() {
            let (key, value) = result?;
            let upper_bound_check = match &self.upper {
                Bound::Included(ref upper) => key.as_ref() <= upper.as_slice(),
                Bound::Excluded(ref upper) => key.as_ref() < upper.as_slice(),
                Bound::Unbounded => true,
            };
            if !upper_bound_check {
                break;
            }
            let lower_bound_check = match &self.lower {
                Bound::Included(ref lower) => key.as_ref() >= lower.as_slice(),
                Bound::Excluded(ref lower) => key.as_ref() > lower.as_slice(),
                Bound::Unbounded => true,
            };
            if lower_bound_check {
                return Ok(Some((Bytes::from(key), Bytes::from(value))));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::db::DataBaseBuilder;
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{
        IndexImplEnum, IndexImplParams, IndexIter, Iter, PrimaryKeyIndexImpl, Storage, Transaction,
    };
    use crate::types::index::{IndexMeta, IndexType};
    use crate::types::tuple::Tuple;
    use crate::types::tuple_builder::TupleIdBuilder;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use itertools::Itertools;
    use std::collections::{Bound, VecDeque};
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_in_rocksdb_storage_works_with_data() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let columns = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            )),
        ]);

        let source_columns = columns
            .iter()
            .map(|col_ref| ColumnCatalog::clone(&col_ref))
            .collect_vec();
        let _ = transaction.create_table(
            &table_cache,
            Arc::new("test".to_string()),
            source_columns,
            false,
        )?;

        let table_catalog = transaction.table(&table_cache, Arc::new("test".to_string()))?;
        assert!(table_catalog.is_some());
        assert!(table_catalog
            .unwrap()
            .get_column_id_by_name(&"c1".to_string())
            .is_some());

        transaction.append_tuple(
            &"test".to_string(),
            Tuple {
                id: Some(DataValue::Int32(Some(1))),
                values: vec![DataValue::Int32(Some(1)), DataValue::Boolean(Some(true))],
            },
            &[LogicalType::Integer, LogicalType::Boolean],
            false,
        )?;
        transaction.append_tuple(
            &"test".to_string(),
            Tuple {
                id: Some(DataValue::Int32(Some(2))),
                values: vec![DataValue::Int32(Some(2)), DataValue::Boolean(Some(false))],
            },
            &[LogicalType::Integer, LogicalType::Boolean],
            false,
        )?;

        let mut iter = transaction.read(
            &table_cache,
            Arc::new("test".to_string()),
            (Some(1), Some(1)),
            vec![(0, columns[0].clone())],
        )?;

        let option_1 = iter.next_tuple()?;
        assert_eq!(option_1.unwrap().id, Some(DataValue::Int32(Some(2))));

        let option_2 = iter.next_tuple()?;
        assert_eq!(option_2, None);

        Ok(())
    }

    #[test]
    fn test_index_iter_pk() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

        let _ = fnck_sql.run("create table t1 (a int primary key)")?;
        let _ = fnck_sql.run("insert into t1 (a) values (0), (1), (2), (3), (4)")?;
        let transaction = fnck_sql.storage.transaction()?;

        let table_name = Arc::new("t1".to_string());
        let table = transaction
            .table(fnck_sql.state.table_cache(), table_name.clone())?
            .unwrap()
            .clone();
        let a_column_id = table.get_column_id_by_name("a").unwrap();
        let tuple_ids = vec![
            DataValue::Int32(Some(0)),
            DataValue::Int32(Some(2)),
            DataValue::Int32(Some(3)),
            DataValue::Int32(Some(4)),
        ];
        let id_builder = TupleIdBuilder::new(table.schema_ref());
        let mut iter = IndexIter {
            offset: 0,
            limit: None,
            id_builder,
            params: IndexImplParams {
                tuple_schema_ref: table.schema_ref().clone(),
                projections: vec![0],
                index_meta: Arc::new(IndexMeta {
                    id: 0,
                    column_ids: vec![*a_column_id],
                    table_name,
                    pk_ty: LogicalType::Integer,
                    name: "pk_a".to_string(),
                    ty: IndexType::PrimaryKey { is_multiple: false },
                }),
                table_name: &table.name,
                table_types: table.types(),
                tx: &transaction,
            },
            ranges: VecDeque::from(vec![
                Range::Eq(DataValue::Int32(Some(0))),
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(Some(2))),
                    max: Bound::Included(DataValue::Int32(Some(4))),
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

    #[test]
    fn test_read_by_index() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let fnck_sql = DataBaseBuilder::path(temp_dir.path()).build()?;
        let _ = fnck_sql.run("create table t1 (a int primary key, b int unique)")?;
        let _ = fnck_sql.run("insert into t1 (a, b) values (0, 0), (1, 1), (2, 2)")?;
        let transaction = fnck_sql.storage.transaction().unwrap();

        let table = transaction
            .table(fnck_sql.state.table_cache(), Arc::new("t1".to_string()))?
            .unwrap()
            .clone();
        let columns = table.columns().cloned().enumerate().collect_vec();
        let mut iter = transaction
            .read_by_index(
                fnck_sql.state.table_cache(),
                Arc::new("t1".to_string()),
                (Some(0), Some(1)),
                columns,
                table.indexes[0].clone(),
                vec![Range::Scope {
                    min: Bound::Excluded(DataValue::Int32(Some(0))),
                    max: Bound::Unbounded,
                }],
            )
            .unwrap();

        while let Some(tuple) = iter.next_tuple()? {
            assert_eq!(tuple.id, Some(DataValue::Int32(Some(1))));
            assert_eq!(
                tuple.values,
                vec![DataValue::Int32(Some(1)), DataValue::Int32(Some(1))]
            )
        }

        Ok(())
    }
}

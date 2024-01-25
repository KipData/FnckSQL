use crate::catalog::{ColumnCatalog, ColumnRef, TableCatalog, TableMeta, TableName};
use crate::expression::simplify::ConstantBinary;
use crate::storage::table_codec::TableCodec;
use crate::storage::{
    tuple_projection, Bounds, IndexIter, Iter, Projections, Storage, StorageError, Transaction,
};
use crate::types::index::{Index, IndexMetaRef};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::ColumnId;
use kip_db::kernel::lsm::iterator::Iter as KipDBIter;
use kip_db::kernel::lsm::mvcc::{CheckType, TransactionIter};
use kip_db::kernel::lsm::storage::Config;
use kip_db::kernel::lsm::{mvcc, storage};
use kip_db::kernel::utils::lru_cache::ShardingLruCache;
use kip_db::KernelError;
use std::collections::hash_map::RandomState;
use std::collections::{Bound, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct KipStorage {
    pub inner: Arc<storage::KipStorage>,
}

impl KipStorage {
    pub async fn new(path: impl Into<PathBuf> + Send) -> Result<Self, StorageError> {
        let storage =
            storage::KipStorage::open_with_config(Config::new(path).enable_level_0_memorization())
                .await?;

        Ok(KipStorage {
            inner: Arc::new(storage),
        })
    }
}

impl Storage for KipStorage {
    type TransactionType = KipTransaction;

    async fn transaction(&self) -> Result<Self::TransactionType, StorageError> {
        let tx = self.inner.new_transaction(CheckType::Optimistic).await;

        Ok(KipTransaction {
            tx,
            cache: ShardingLruCache::new(8, 2, RandomState::default())?,
        })
    }
}

pub struct KipTransaction {
    tx: mvcc::Transaction,
    cache: ShardingLruCache<String, TableCatalog>,
}

impl Transaction for KipTransaction {
    type IterType<'a> = KipIter<'a>;

    fn read(
        &self,
        table_name: TableName,
        bounds: Bounds,
        projections: Projections,
    ) -> Result<Self::IterType<'_>, StorageError> {
        let all_columns = self
            .table(table_name.clone())
            .ok_or(StorageError::TableNotFound)?
            .all_columns();
        let (min, max) = TableCodec::tuple_bound(&table_name);
        let iter = self.tx.iter(Bound::Included(&min), Bound::Included(&max))?;

        Ok(KipIter {
            offset: bounds.0.unwrap_or(0),
            limit: bounds.1,
            projections,
            all_columns,
            iter,
        })
    }

    fn read_by_index(
        &self,
        table_name: TableName,
        (offset_option, limit_option): Bounds,
        projections: Projections,
        index_meta: IndexMetaRef,
        binaries: Vec<ConstantBinary>,
    ) -> Result<IndexIter<'_>, StorageError> {
        let table = self
            .table(table_name.clone())
            .ok_or(StorageError::TableNotFound)?;
        let offset = offset_option.unwrap_or(0);

        Ok(IndexIter {
            offset,
            limit: limit_option,
            projections,
            index_meta,
            table,
            index_values: VecDeque::new(),
            binaries: VecDeque::from(binaries),
            tx: &self.tx,
            scope_iter: None,
        })
    }

    fn add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_ids: Vec<TupleId>,
        is_unique: bool,
    ) -> Result<(), StorageError> {
        let (key, value) = TableCodec::encode_index(table_name, &index, &tuple_ids)?;

        if let Some(bytes) = self.tx.get(&key)? {
            if is_unique {
                let old_tuple_ids = TableCodec::decode_index(&bytes)?;

                if old_tuple_ids[0] != tuple_ids[0] {
                    return Err(StorageError::DuplicateUniqueValue);
                } else {
                    return Ok(());
                }
            } else {
                todo!("联合索引")
            }
        }

        self.tx.set(key, value);

        Ok(())
    }

    fn del_index(&mut self, table_name: &str, index: &Index) -> Result<(), StorageError> {
        let key = TableCodec::encode_index_key(table_name, index)?;

        self.tx.remove(&key)?;

        Ok(())
    }

    fn append(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        is_overwrite: bool,
    ) -> Result<(), StorageError> {
        let (key, value) = TableCodec::encode_tuple(table_name, &tuple)?;

        if !is_overwrite && self.tx.get(&key)?.is_some() {
            return Err(StorageError::DuplicatePrimaryKey);
        }
        self.tx.set(key, value);

        Ok(())
    }

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<(), StorageError> {
        let key = TableCodec::encode_tuple_key(table_name, &tuple_id)?;
        self.tx.remove(&key)?;

        Ok(())
    }

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId, StorageError> {
        if let Some(mut catalog) = self.table(table_name.clone()).cloned() {
            if !column.nullable && column.default_value().is_none() {
                return Err(StorageError::NeedNullAbleOrDefault);
            }

            for col in catalog.all_columns() {
                if col.name() == column.name() {
                    return if if_not_exists {
                        Ok(col.id().unwrap())
                    } else {
                        Err(StorageError::DuplicateColumn)
                    };
                }
            }

            let col_id = catalog.add_column(column.clone())?;

            if column.desc.is_unique {
                let meta_ref = catalog.add_index_meta(
                    format!("uk_{}", column.name()),
                    vec![col_id],
                    true,
                    false,
                );
                let (key, value) = TableCodec::encode_index_meta(table_name, meta_ref)?;
                self.tx.set(key, value);
            }

            let column = catalog.get_column_by_id(&col_id).unwrap();
            let (key, value) = TableCodec::encode_column(&table_name, column)?;
            self.tx.set(key, value);
            self.cache.remove(table_name);

            Ok(col_id)
        } else {
            Err(StorageError::TableNotFound)
        }
    }

    fn drop_column(
        &mut self,
        table_name: &TableName,
        column_name: &str,
        if_exists: bool,
    ) -> Result<(), StorageError> {
        if let Some(catalog) = self.table(table_name.clone()).cloned() {
            let column = catalog.get_column_by_name(column_name).unwrap();

            if let Some(index_meta) = catalog.get_unique_index(&column.id().unwrap()) {
                let (index_meta_key, _) = TableCodec::encode_index_meta(table_name, index_meta)?;
                self.tx.remove(&index_meta_key)?;

                let (index_min, index_max) = TableCodec::index_bound(table_name, &index_meta.id);
                Self::_drop_data(&mut self.tx, &index_min, &index_max)?;
            }
            let (key, _) = TableCodec::encode_column(&table_name, column)?;

            match self.tx.remove(&key) {
                Ok(_) => (),
                Err(KernelError::KeyNotFound) => {
                    if !if_exists {
                        Err(KernelError::KeyNotFound)?;
                    }
                }
                err => err?,
            }
            self.cache.remove(table_name);

            Ok(())
        } else {
            Err(StorageError::TableNotFound)
        }
    }

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName, StorageError> {
        let (table_key, value) =
            TableCodec::encode_root_table(&TableMeta::empty(table_name.clone()))?;
        if self.tx.get(&table_key)?.is_some() {
            if if_not_exists {
                return Ok(table_name);
            }
            return Err(StorageError::TableExists);
        }
        self.tx.set(table_key, value);

        let mut table_catalog = TableCatalog::new(table_name.clone(), columns)?;

        Self::create_index_meta_for_table(&mut self.tx, &mut table_catalog)?;

        for column in table_catalog.columns.values() {
            let (key, value) = TableCodec::encode_column(&table_name, column)?;
            self.tx.set(key, value);
        }
        self.cache.put(table_name.to_string(), table_catalog);

        Ok(table_name)
    }

    fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), StorageError> {
        if self.table(Arc::new(table_name.to_string())).is_none() {
            if if_exists {
                return Ok(());
            } else {
                return Err(StorageError::TableNotFound);
            }
        }
        self.drop_data(table_name)?;

        let (column_min, column_max) = TableCodec::columns_bound(table_name);
        Self::_drop_data(&mut self.tx, &column_min, &column_max)?;

        let (index_meta_min, index_meta_max) = TableCodec::index_meta_bound(table_name);
        Self::_drop_data(&mut self.tx, &index_meta_min, &index_meta_max)?;

        self.tx
            .remove(&TableCodec::encode_root_table_key(table_name))?;

        let _ = self.cache.remove(&table_name.to_string());

        Ok(())
    }

    fn drop_data(&mut self, table_name: &str) -> Result<(), StorageError> {
        let (tuple_min, tuple_max) = TableCodec::tuple_bound(table_name);
        Self::_drop_data(&mut self.tx, &tuple_min, &tuple_max)?;

        let (index_min, index_max) = TableCodec::all_index_bound(table_name);
        Self::_drop_data(&mut self.tx, &index_min, &index_max)?;

        Ok(())
    }

    fn table(&self, table_name: TableName) -> Option<&TableCatalog> {
        let mut option = self.cache.get(&table_name);

        if option.is_none() {
            // TODO: unify the data into a `Meta` prefix and use one iteration to collect all data
            let (columns, indexes) = Self::table_collect(table_name.clone(), &self.tx).ok()?;

            if let Ok(catalog) = TableCatalog::reload(table_name.clone(), columns, indexes) {
                option = self
                    .cache
                    .get_or_insert(table_name.to_string(), |_| Ok(catalog))
                    .ok();
            }
        }

        option
    }

    fn table_metas(&self) -> Result<Vec<TableMeta>, StorageError> {
        let mut metas = vec![];
        let (min, max) = TableCodec::root_table_bound();
        let mut iter = self.tx.iter(Bound::Included(&min), Bound::Included(&max))?;

        while let Some((_, value_option)) = iter.try_next().ok().flatten() {
            if let Some(value) = value_option {
                let meta = TableCodec::decode_root_table(&value)?;

                metas.push(meta);
            }
        }

        Ok(metas)
    }

    fn save_meta(&mut self, table_meta: &TableMeta) -> Result<(), StorageError> {
        let (key, value) = TableCodec::encode_root_table(table_meta)?;
        self.tx.set(key, value);

        Ok(())
    }

    fn histogram_paths(&self, table_name: &str) -> Result<Vec<String>, StorageError> {
        if let Some(bytes) = self
            .tx
            .get(&TableCodec::encode_root_table_key(table_name))?
        {
            let meta = TableCodec::decode_root_table(&bytes)?;

            return Ok(meta.histogram_paths);
        }

        Ok(vec![])
    }

    async fn commit(self) -> Result<(), StorageError> {
        self.tx.commit().await?;

        Ok(())
    }
}

impl KipTransaction {
    fn table_collect(
        table_name: TableName,
        tx: &mvcc::Transaction,
    ) -> Result<(Vec<ColumnCatalog>, Vec<IndexMetaRef>), StorageError> {
        let (table_min, table_max) = TableCodec::table_bound(&table_name);
        let mut column_iter = tx.iter(Bound::Included(&table_min), Bound::Included(&table_max))?;

        let mut columns = Vec::new();
        let mut index_metas = Vec::new();

        // Tips: only `Column`, `IndexMeta`, `TableMeta`
        while let Some((key, value_option)) = column_iter.try_next().ok().flatten() {
            if let Some(value) = value_option {
                if key.starts_with(&table_min) {
                    columns.push(TableCodec::decode_column(&value)?);
                } else {
                    index_metas.push(Arc::new(TableCodec::decode_index_meta(&value)?));
                }
            }
        }

        Ok((columns, index_metas))
    }

    fn _drop_data(tx: &mut mvcc::Transaction, min: &[u8], max: &[u8]) -> Result<(), StorageError> {
        let mut iter = tx.iter(Bound::Included(min), Bound::Included(max))?;
        let mut data_keys = vec![];

        while let Some((key, value_option)) = iter.try_next()? {
            if value_option.is_some() {
                data_keys.push(key);
            }
        }
        drop(iter);

        for key in data_keys {
            tx.remove(&key)?
        }

        Ok(())
    }

    fn create_index_meta_for_table(
        tx: &mut mvcc::Transaction,
        table: &mut TableCatalog,
    ) -> Result<(), StorageError> {
        let table_name = table.name.clone();

        for col in table
            .all_columns()
            .into_iter()
            .filter(|col| col.desc.is_index())
        {
            let is_primary = col.desc.is_primary;
            // FIXME: composite indexes may exist on future
            let prefix = if is_primary { "pk" } else { "uk" };

            if let Some(col_id) = col.id() {
                let meta_ref = table.add_index_meta(
                    format!("{}_{}", prefix, col.name()),
                    vec![col_id],
                    col.desc.is_unique,
                    is_primary,
                );
                let (key, value) = TableCodec::encode_index_meta(&table_name, meta_ref)?;

                tx.set(key, value);
            }
        }
        Ok(())
    }
}

pub struct KipIter<'a> {
    offset: usize,
    limit: Option<usize>,
    projections: Projections,
    all_columns: Vec<ColumnRef>,
    iter: TransactionIter<'a>,
}

impl Iter for KipIter<'_> {
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

        while let Some(item) = self.iter.try_next()? {
            if let (_, Some(value)) = item {
                let tuple = tuple_projection(
                    &mut self.limit,
                    &self.projections,
                    TableCodec::decode_tuple(self.all_columns.clone(), &value),
                )?;

                return Ok(Some(tuple));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::{Database, DatabaseError};
    use crate::expression::simplify::ConstantBinary;
    use crate::expression::ScalarExpression;
    use crate::storage::kip::KipStorage;
    use crate::storage::{IndexIter, Iter, Storage, StorageError, Transaction};
    use crate::types::index::IndexMeta;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use itertools::Itertools;
    use std::collections::{Bound, VecDeque};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_in_kipdb_storage_works_with_data() -> Result<(), StorageError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = KipStorage::new(temp_dir.path()).await?;
        let mut transaction = storage.transaction().await?;
        let columns = vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false, None),
                None,
            )),
        ];

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
                columns: columns.clone(),
                values: vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::Boolean(Some(true))),
                ],
            },
            false,
        )?;
        transaction.append(
            &"test".to_string(),
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(2)))),
                columns: columns.clone(),
                values: vec![
                    Arc::new(DataValue::Int32(Some(2))),
                    Arc::new(DataValue::Boolean(Some(false))),
                ],
            },
            false,
        )?;

        let mut iter = transaction.read(
            Arc::new("test".to_string()),
            (Some(1), Some(1)),
            vec![ScalarExpression::ColumnRef(columns[0].clone())],
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
        let kipsql = Database::with_kipdb(temp_dir.path()).await?;

        let _ = kipsql.run("create table t1 (a int primary key)").await?;
        let _ = kipsql
            .run("insert into t1 (a) values (0), (1), (2), (3), (4)")
            .await?;
        let transaction = kipsql.storage.transaction().await?;

        let table = transaction
            .table(Arc::new("t1".to_string()))
            .unwrap()
            .clone();
        let projections = table
            .all_columns()
            .into_iter()
            .map(|col| ScalarExpression::ColumnRef(col))
            .collect_vec();
        let tuple_ids = vec![
            Arc::new(DataValue::Int32(Some(0))),
            Arc::new(DataValue::Int32(Some(2))),
            Arc::new(DataValue::Int32(Some(3))),
            Arc::new(DataValue::Int32(Some(4))),
        ];
        let mut iter = IndexIter {
            offset: 0,
            limit: None,
            projections,
            index_meta: Arc::new(IndexMeta {
                id: 0,
                column_ids: vec![0],
                name: "pk_a".to_string(),
                is_unique: false,
                is_primary: true,
            }),
            table: &table,
            binaries: VecDeque::from(vec![
                ConstantBinary::Eq(Arc::new(DataValue::Int32(Some(0)))),
                ConstantBinary::Scope {
                    min: Bound::Included(Arc::new(DataValue::Int32(Some(2)))),
                    max: Bound::Included(Arc::new(DataValue::Int32(Some(4)))),
                },
            ]),
            index_values: VecDeque::new(),
            tx: &transaction.tx,
            scope_iter: None,
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
        let kipsql = Database::with_kipdb(temp_dir.path()).await?;
        let _ = kipsql
            .run("create table t1 (a int primary key, b int unique)")
            .await?;
        let _ = kipsql
            .run("insert into t1 (a, b) values (0, 0), (1, 1), (2, 2)")
            .await?;
        let transaction = kipsql.storage.transaction().await.unwrap();

        let table = transaction
            .table(Arc::new("t1".to_string()))
            .unwrap()
            .clone();
        let projections = table
            .all_columns()
            .into_iter()
            .map(|col| ScalarExpression::ColumnRef(col))
            .collect_vec();
        let mut iter = transaction
            .read_by_index(
                Arc::new("t1".to_string()),
                (Some(0), Some(1)),
                projections,
                table.indexes[0].clone(),
                vec![ConstantBinary::Scope {
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

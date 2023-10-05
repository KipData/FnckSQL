use crate::catalog::{ColumnCatalog, TableCatalog, TableName};
use crate::expression::simplify::ConstantBinary;
use crate::storage::table_codec::TableCodec;
use crate::storage::{
    tuple_projection, Bounds, IndexIter, Iter, Projections, Storage, StorageError, Transaction,
};
use crate::types::errors::TypeError;
use crate::types::index::{Index, IndexMeta, IndexMetaRef};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use async_trait::async_trait;
use kip_db::kernel::lsm::iterator::Iter as KipDBIter;
use kip_db::kernel::lsm::mvcc::TransactionIter;
use kip_db::kernel::lsm::storage::Config;
use kip_db::kernel::lsm::{mvcc, storage};
use kip_db::kernel::utils::lru_cache::ShardingLruCache;
use kip_db::kernel::Storage as KipDBStorage;
use std::collections::hash_map::RandomState;
use std::collections::{Bound, VecDeque};
use std::io::Write;
use std::mem;
use std::ops::SubAssign;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct KipStorage {
    cache: Arc<ShardingLruCache<String, TableCatalog>>,
    pub inner: Arc<storage::KipStorage>,
}

impl KipStorage {
    pub async fn new(path: impl Into<PathBuf> + Send) -> Result<Self, StorageError> {
        let config = Config::new(path);
        let storage = storage::KipStorage::open_with_config(config).await?;

        Ok(KipStorage {
            cache: Arc::new(ShardingLruCache::new(32, 16, RandomState::default())?),
            inner: Arc::new(storage),
        })
    }

    fn column_collect(
        name: &String,
        tx: &mvcc::Transaction,
    ) -> Result<(Vec<ColumnCatalog>, Option<TableName>), StorageError> {
        let (column_min, column_max) = TableCodec::columns_bound(name);
        let mut column_iter =
            tx.iter(Bound::Included(&column_min), Bound::Included(&column_max))?;

        let mut columns = vec![];
        let mut name_option = None;

        while let Some((_, value_option)) = column_iter.try_next().ok().flatten() {
            if let Some(value) = value_option {
                let (table_name, column) = TableCodec::decode_column(&value)?;

                if name != table_name.as_str() {
                    return Ok((vec![], None));
                }
                let _ = name_option.insert(table_name);

                columns.push(column);
            }
        }

        Ok((columns, name_option))
    }

    fn index_meta_collect(name: &String, tx: &mvcc::Transaction) -> Option<Vec<IndexMetaRef>> {
        let (index_min, index_max) = TableCodec::index_meta_bound(name);
        let mut index_metas = vec![];
        let mut index_iter = tx
            .iter(Bound::Included(&index_min), Bound::Included(&index_max))
            .ok()?;

        while let Some((_, value_option)) = index_iter.try_next().ok().flatten() {
            if let Some(value) = value_option {
                if let Some(index_meta) = TableCodec::decode_index_meta(&value).ok() {
                    index_metas.push(Arc::new(index_meta));
                }
            }
        }

        Some(index_metas)
    }

    fn _drop_data(table: &mut KipTransaction, min: &[u8], max: &[u8]) -> Result<(), StorageError> {
        let mut iter = table
            .tx
            .iter(Bound::Included(&min), Bound::Included(&max))?;
        let mut data_keys = vec![];

        while let Some((key, value_option)) = iter.try_next()? {
            if value_option.is_some() {
                data_keys.push(key);
            }
        }
        drop(iter);

        for key in data_keys {
            table.tx.remove(&key)?
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
            .filter(|col| col.desc.is_unique)
        {
            if let Some(col_id) = col.id {
                let meta = IndexMeta {
                    id: 0,
                    column_ids: vec![col_id],
                    name: format!("uk_{}", col.name),
                    is_unique: true,
                };
                let meta_ref = table.add_index_meta(meta);
                let (key, value) = TableCodec::encode_index_meta(&table_name, meta_ref)?;

                tx.set(key, value);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Storage for KipStorage {
    type TransactionType = KipTransaction;

    async fn create_table(
        &self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
    ) -> Result<TableName, StorageError> {
        let mut tx = self.inner.new_transaction().await;
        let mut table_catalog = TableCatalog::new(table_name.clone(), columns)?;

        Self::create_index_meta_for_table(&mut tx, &mut table_catalog)?;

        for (_, column) in &table_catalog.columns {
            let (key, value) = TableCodec::encode_column(column)?;
            tx.set(key, value);
        }

        let (k, v) = TableCodec::encode_root_table(&table_name)?;
        self.inner.set(k, v).await?;

        tx.commit().await?;
        self.cache.put(table_name.to_string(), table_catalog);

        Ok(table_name)
    }

    async fn drop_table(&self, name: &String) -> Result<(), StorageError> {
        self.drop_data(name).await?;

        let (min, max) = TableCodec::columns_bound(name);
        let mut tx = self.inner.new_transaction().await;
        let mut iter = tx.iter(Bound::Included(&min), Bound::Included(&max))?;
        let mut col_keys = vec![];

        while let Some((key, value_option)) = iter.try_next()? {
            if value_option.is_some() {
                col_keys.push(key);
            }
        }
        drop(iter);

        for col_key in col_keys {
            tx.remove(&col_key)?
        }
        tx.remove(&TableCodec::encode_root_table_key(name))?;
        tx.commit().await?;

        let _ = self.cache.remove(name);

        Ok(())
    }

    async fn drop_data(&self, name: &String) -> Result<(), StorageError> {
        if let Some(mut transaction) = self.transaction(name).await {
            let (tuple_min, tuple_max) = transaction.table_codec.tuple_bound();
            Self::_drop_data(&mut transaction, &tuple_min, &tuple_max)?;

            let (index_min, index_max) = transaction.table_codec.all_index_bound();
            Self::_drop_data(&mut transaction, &index_min, &index_max)?;

            transaction.tx.commit().await?;
        }

        Ok(())
    }

    async fn transaction(&self, name: &String) -> Option<Self::TransactionType> {
        let table_codec = self.table(name).await.map(|catalog| TableCodec {
            table: catalog.clone(),
        })?;
        let tx = self.inner.new_transaction().await;

        Some(KipTransaction { table_codec, tx })
    }

    async fn table(&self, name: &String) -> Option<&TableCatalog> {
        let mut option = self.cache.get(name);

        if option.is_none() {
            let tx = self.inner.new_transaction().await;
            // TODO: unify the data into a `Meta` prefix and use one iteration to collect all data
            let (columns, name_option) = Self::column_collect(name, &tx).ok()?;
            let indexes = Self::index_meta_collect(name, &tx)?;

            if let Some(catalog) = name_option.and_then(|table_name| {
                TableCatalog::new_with_indexes(table_name, columns, indexes).ok()
            }) {
                option = self
                    .cache
                    .get_or_insert(name.to_string(), |_| Ok(catalog))
                    .ok();
            }
        }

        option
    }

    async fn show_tables(&self) -> Result<Vec<String>, StorageError> {
        let mut tables = vec![];
        let (min, max) = TableCodec::root_table_bound();

        let tx = self.inner.new_transaction().await;
        let mut iter = tx.iter(Bound::Included(&min), Bound::Included(&max))?;

        while let Some((_, value_option)) = iter.try_next().ok().flatten() {
            if let Some(value) = value_option {
                let table_name = TableCodec::decode_root_table(&value)?;

                tables.push(table_name);
            }
        }

        Ok(tables)
    }
}

pub struct KipTransaction {
    table_codec: TableCodec,
    tx: mvcc::Transaction,
}

#[async_trait]
impl Transaction for KipTransaction {
    type IterType<'a> = KipIter<'a>;

    fn read(
        &self,
        bounds: Bounds,
        projections: Projections,
    ) -> Result<Self::IterType<'_>, StorageError> {
        let (min, max) = self.table_codec.tuple_bound();
        let iter = self.tx.iter(Bound::Included(&min), Bound::Included(&max))?;

        Ok(KipIter {
            offset: bounds.0.unwrap_or(0),
            limit: bounds.1,
            projections,
            table_codec: &self.table_codec,
            iter,
        })
    }

    fn read_by_index(
        &self,
        (offset_option, mut limit_option): Bounds,
        projections: Projections,
        index_meta: IndexMetaRef,
        binaries: Vec<ConstantBinary>,
    ) -> Result<IndexIter<'_>, StorageError> {
        let mut tuple_ids = Vec::new();
        let mut offset = offset_option.unwrap_or(0);

        for binary in binaries {
            if matches!(limit_option, Some(0)) {
                break;
            }

            match binary {
                ConstantBinary::Scope { min, max } => {
                    let mut iter = self.scope_to_iter(&index_meta, min, max)?;

                    while let Some((_, value_option)) = iter.try_next()? {
                        if let Some(value) = value_option {
                            for id in TableCodec::decode_index(&value)? {
                                if Self::offset_move(&mut offset) {
                                    continue;
                                }

                                tuple_ids.push(id);

                                if Self::limit_move(&mut limit_option) {
                                    break;
                                }
                            }
                        }

                        if matches!(limit_option, Some(0)) {
                            break;
                        }
                    }
                }
                ConstantBinary::Eq(val) => {
                    if Self::offset_move(&mut offset) {
                        continue;
                    }

                    let key = self.val_to_key(&index_meta, val)?;

                    if let Some(bytes) = self.tx.get(&key)? {
                        tuple_ids.append(&mut TableCodec::decode_index(&bytes)?)
                    }

                    let _ = Self::limit_move(&mut limit_option);
                }
                _ => (),
            }
        }

        Ok(IndexIter {
            projections,
            table_codec: &self.table_codec,
            tuple_ids: VecDeque::from(tuple_ids),
            tx: &self.tx,
        })
    }

    fn add_index(
        &mut self,
        index: Index,
        tuple_ids: Vec<TupleId>,
        is_unique: bool,
    ) -> Result<(), StorageError> {
        let (key, value) = self.table_codec.encode_index(&index, &tuple_ids)?;

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

    fn del_index(&mut self, index: &Index) -> Result<(), StorageError> {
        let key = self.table_codec.encode_index_key(&index)?;

        self.tx.remove(&key)?;

        Ok(())
    }

    fn append(&mut self, tuple: Tuple, is_overwrite: bool) -> Result<(), StorageError> {
        let (key, value) = self.table_codec.encode_tuple(&tuple)?;

        if !is_overwrite && self.tx.get(&key)?.is_some() {
            return Err(StorageError::DuplicatePrimaryKey);
        }

        // create or overwrite text file
        for (i, col) in tuple.columns.iter().enumerate() {
            if matches!(col.desc.column_datatype, LogicalType::Text) {
                let value = tuple.values[i].clone();
                let file_path = generate_text_file_path(
                    tuple.id.as_ref().expect("The tuple has no primary key"),
                    col,
                );
                let mut file = std::fs::File::create(file_path)?;
                match value.as_ref() {
                    DataValue::Utf8(text) => {
                        file.write_all(
                            text.as_ref().map_or("".as_bytes(), |text| text.as_bytes()),
                        )?;
                    }
                    DataValue::Null => {}
                    _ => {
                        panic!("The data type of text column should be Utf8 or Null");
                    }
                }
                // TODO do we need to wait for flushing?
                file.flush()?;
            }
        }

        self.tx.set(key, value);

        Ok(())
    }

    fn delete(&mut self, tuple_id: TupleId) -> Result<(), StorageError> {
        let key = self.table_codec.encode_tuple_key(&tuple_id)?;
        self.tx.remove(&key)?;

        Ok(())
    }

    async fn commit(self) -> Result<(), StorageError> {
        self.tx.commit().await?;

        Ok(())
    }
}

impl KipTransaction {
    fn val_to_key(&self, index_meta: &IndexMetaRef, val: ValueRef) -> Result<Vec<u8>, TypeError> {
        let index = Index::new(index_meta.id, vec![val]);

        self.table_codec.encode_index_key(&index)
    }

    fn scope_to_iter(
        &self,
        index_meta: &IndexMetaRef,
        min: Bound<ValueRef>,
        max: Bound<ValueRef>,
    ) -> Result<TransactionIter, StorageError> {
        let bound_encode = |bound: Bound<ValueRef>| -> Result<_, StorageError> {
            match bound {
                Bound::Included(val) => Ok(Bound::Included(self.val_to_key(&index_meta, val)?)),
                Bound::Excluded(val) => Ok(Bound::Excluded(self.val_to_key(&index_meta, val)?)),
                Bound::Unbounded => Ok(Bound::Unbounded),
            }
        };
        let check_bound = |value: &mut Bound<Vec<u8>>, bound: Vec<u8>| {
            if matches!(value, Bound::Unbounded) {
                let _ = mem::replace(value, Bound::Included(bound));
            }
        };
        let (bound_min, bound_max) = self.table_codec.index_bound(&index_meta.id);

        let mut encode_min = bound_encode(min)?;
        check_bound(&mut encode_min, bound_min);

        let mut encode_max = bound_encode(max)?;
        check_bound(&mut encode_max, bound_max);

        Ok(self.tx.iter(
            encode_min.as_ref().map(Vec::as_slice),
            encode_max.as_ref().map(Vec::as_slice),
        )?)
    }

    fn offset_move(offset: &mut usize) -> bool {
        if *offset > 0 {
            offset.sub_assign(1);

            true
        } else {
            false
        }
    }

    fn limit_move(limit_option: &mut Option<usize>) -> bool {
        if let Some(limit) = limit_option {
            limit.sub_assign(1);

            return *limit == 0;
        }

        false
    }
}

pub struct KipIter<'a> {
    offset: usize,
    limit: Option<usize>,
    projections: Projections,
    table_codec: &'a TableCodec,
    iter: TransactionIter<'a>,
}

pub fn generate_text_file_path(tuple_id: &TupleId, col: &ColumnCatalog) -> String {
    let col_id = col.id.expect("The column id should not be none");
    let table_name = col
        .table_name
        .as_ref()
        .expect("The table name should not be none");
    // TODO support multi databases
    // TODO support specify home dir
    format!("data/text-{}-{}-{}", table_name, tuple_id, col_id)
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
                    self.table_codec.decode_tuple(&value),
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
    use crate::storage::memory::test::data_filling;
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{IndexIter, Iter, Storage, StorageError, Transaction};
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
        let columns = vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false),
                None,
            )),
        ];

        let source_columns = columns
            .iter()
            .map(|col_ref| ColumnCatalog::clone(&col_ref))
            .collect_vec();
        let table_id = storage
            .create_table(Arc::new("test".to_string()), source_columns)
            .await?;

        let table_catalog = storage.table(&"test".to_string()).await;
        assert!(table_catalog.is_some());
        assert!(table_catalog
            .unwrap()
            .get_column_id_by_name(&"c1".to_string())
            .is_some());

        let mut transaction = storage.transaction(&table_id).await.unwrap();
        data_filling(columns, &mut transaction)?;

        let mut iter = transaction.read(
            (Some(1), Some(1)),
            vec![ScalarExpression::InputRef {
                index: 0,
                ty: LogicalType::Integer,
            }],
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
    async fn test_index_iter() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kipsql = Database::with_kipdb(temp_dir.path()).await?;

        let _ = kipsql.run("create table t1 (a int primary key)").await?;
        let _ = kipsql
            .run("insert into t1 (a) values (0), (1), (2)")
            .await?;

        let table = kipsql
            .storage
            .table(&"t1".to_string())
            .await
            .unwrap()
            .clone();
        let projections = table
            .all_columns()
            .into_iter()
            .map(|col| ScalarExpression::ColumnRef(col))
            .collect_vec();
        let codec = TableCodec { table };
        let tx = kipsql.storage.transaction(&"t1".to_string()).await.unwrap();
        let tuple_ids = vec![
            Arc::new(DataValue::Int32(Some(0))),
            Arc::new(DataValue::Int32(Some(1))),
            Arc::new(DataValue::Int32(Some(2))),
        ];
        let mut iter = IndexIter {
            projections,
            table_codec: &codec,
            tuple_ids: VecDeque::from(tuple_ids.clone()),
            tx: &tx.tx,
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

        let table = kipsql
            .storage
            .table(&"t1".to_string())
            .await
            .unwrap()
            .clone();
        let projections = table
            .all_columns()
            .into_iter()
            .map(|col| ScalarExpression::ColumnRef(col))
            .collect_vec();
        let transaction = kipsql.storage.transaction(&"t1".to_string()).await.unwrap();
        let mut iter = transaction
            .read_by_index(
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

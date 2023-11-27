use crate::catalog::{ColumnCatalog, ColumnRef, TableCatalog, TableName};
use crate::expression::simplify::ConstantBinary;
use crate::planner::operator::alter_table::{AddColumn, AlterTableOperator};
use crate::storage::table_codec::TableCodec;
use crate::storage::{
    tuple_projection, Bounds, IndexIter, Iter, Projections, Storage, StorageError, Transaction,
};
use crate::types::errors::TypeError;
use crate::types::index::{Index, IndexMeta, IndexMetaRef};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::ValueRef;
use kip_db::kernel::lsm::iterator::Iter as KipDBIter;
use kip_db::kernel::lsm::mvcc::TransactionIter;
use kip_db::kernel::lsm::storage::Config;
use kip_db::kernel::lsm::{mvcc, storage};
use kip_db::kernel::utils::lru_cache::ShardingLruCache;
use std::collections::hash_map::RandomState;
use std::collections::{Bound, VecDeque};
use std::mem;
use std::ops::SubAssign;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct KipStorage {
    pub inner: Arc<storage::KipStorage>,
}

impl KipStorage {
    pub async fn new(path: impl Into<PathBuf> + Send) -> Result<Self, StorageError> {
        let config = Config::new(path);
        let storage = storage::KipStorage::open_with_config(config).await?;

        Ok(KipStorage {
            inner: Arc::new(storage),
        })
    }
}

impl Storage for KipStorage {
    type TransactionType = KipTransaction;

    async fn transaction(&self) -> Result<Self::TransactionType, StorageError> {
        let tx = self.inner.new_transaction().await;

        Ok(KipTransaction {
            tx,
            cache: ShardingLruCache::new(32, 16, RandomState::default())?,
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
        table_name: &String,
        bounds: Bounds,
        projections: Projections,
    ) -> Result<Self::IterType<'_>, StorageError> {
        let all_columns = self
            .table(table_name)
            .ok_or(StorageError::TableNotFound)?
            .all_columns();
        let (min, max) = TableCodec::tuple_bound(table_name);
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
        table_name: &String,
        (offset_option, mut limit_option): Bounds,
        projections: Projections,
        index_meta: IndexMetaRef,
        binaries: Vec<ConstantBinary>,
    ) -> Result<IndexIter<'_>, StorageError> {
        let table = self.table(table_name).ok_or(StorageError::TableNotFound)?;
        let mut tuple_ids = Vec::new();
        let mut offset = offset_option.unwrap_or(0);

        for binary in binaries {
            if matches!(limit_option, Some(0)) {
                break;
            }

            match binary {
                ConstantBinary::Scope { min, max } => {
                    let mut iter = self.scope_to_iter(table_name, &index_meta, min, max)?;

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

                    let key = Self::val_to_key(table_name, &index_meta, val)?;

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
            table,
            tuple_ids: VecDeque::from(tuple_ids),
            tx: &self.tx,
        })
    }

    fn add_index(
        &mut self,
        table_name: &String,
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

    fn del_index(&mut self, table_name: &String, index: &Index) -> Result<(), StorageError> {
        let key = TableCodec::encode_index_key(table_name, &index)?;

        self.tx.remove(&key)?;

        Ok(())
    }

    fn append(
        &mut self,
        table_name: &String,
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

    fn delete(&mut self, table_name: &String, tuple_id: TupleId) -> Result<(), StorageError> {
        let key = TableCodec::encode_tuple_key(table_name, &tuple_id)?;
        self.tx.remove(&key)?;

        Ok(())
    }

    fn alter_table(&mut self, op: &AlterTableOperator) -> Result<(), StorageError> {
        match op {
            AlterTableOperator::AddColumn(AddColumn {
                table_name,
                if_not_exists,
                column,
            }) => {
                // we need catalog generate col_id && index_id
                // generally catalog is immutable, so do not worry it changed when alter table going on
                if let Some(mut catalog) = self.table(table_name).cloned() {
                    // not yet supported default value
                    if !column.nullable {
                        return Err(StorageError::NeedNullAble);
                    }

                    for col in catalog.all_columns() {
                        if col.name() == column.name() {
                            if *if_not_exists {
                                return Ok(())
                            } else {
                                return Err(StorageError::DuplicateColumn);
                            }
                        }
                    }

                    let col_id = catalog.add_column(column.clone())?;

                    if column.desc.is_unique {
                        let meta = IndexMeta {
                            id: 0,
                            column_ids: vec![col_id],
                            name: format!("uk_{}", column.name()),
                            is_unique: true,
                        };
                        let meta_ref = catalog.add_index_meta(meta);
                        let (key, value) = TableCodec::encode_index_meta(table_name, meta_ref)?;
                        self.tx.set(key, value);
                    }

                    let column = catalog.get_column_by_id(&col_id).unwrap();
                    let (key, value) = TableCodec::encode_column(column)?;
                    self.tx.set(key, value);

                    Ok(())
                } else {
                    return Err(StorageError::TableNotFound);
                }
            },
            AlterTableOperator::DropColumn => todo!(),
            AlterTableOperator::DropPrimaryKey => todo!(),
            AlterTableOperator::RenameColumn => todo!(),
            AlterTableOperator::RenameTable => todo!(),
            AlterTableOperator::ChangeColumn => todo!(),
            AlterTableOperator::AlterColumn => todo!(),
        }
    }

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
    ) -> Result<TableName, StorageError> {
        let mut table_catalog = TableCatalog::new(table_name.clone(), columns)?;

        Self::create_index_meta_for_table(&mut self.tx, &mut table_catalog)?;

        for (_, column) in &table_catalog.columns {
            let (key, value) = TableCodec::encode_column(column)?;
            self.tx.set(key, value);
        }
        let (table_key, value) = TableCodec::encode_root_table(&table_name)?;
        self.tx.set(table_key, value);

        self.cache.put(table_name.to_string(), table_catalog);

        Ok(table_name)
    }

    fn drop_table(&mut self, table_name: &String) -> Result<(), StorageError> {
        self.drop_data(table_name)?;

        let (min, max) = TableCodec::columns_bound(table_name);
        let mut iter = self.tx.iter(Bound::Included(&min), Bound::Included(&max))?;
        let mut col_keys = vec![];

        while let Some((key, value_option)) = iter.try_next()? {
            if value_option.is_some() {
                col_keys.push(key);
            }
        }
        drop(iter);

        for col_key in col_keys {
            self.tx.remove(&col_key)?
        }
        self.tx
            .remove(&TableCodec::encode_root_table_key(table_name))?;

        let _ = self.cache.remove(table_name);

        Ok(())
    }

    fn drop_data(&mut self, table_name: &String) -> Result<(), StorageError> {
        let (tuple_min, tuple_max) = TableCodec::tuple_bound(table_name);
        Self::_drop_data(&mut self.tx, &tuple_min, &tuple_max)?;

        let (index_min, index_max) = TableCodec::all_index_bound(table_name);
        Self::_drop_data(&mut self.tx, &index_min, &index_max)?;

        Ok(())
    }

    fn table(&self, table_name: &String) -> Option<&TableCatalog> {
        let mut option = self.cache.get(table_name);

        if option.is_none() {
            // TODO: unify the data into a `Meta` prefix and use one iteration to collect all data
            let (columns, name_option) = Self::column_collect(table_name, &self.tx).ok()?;
            let indexes = Self::index_meta_collect(table_name, &self.tx)?;

            if let Some(catalog) = name_option.and_then(|table_name| {
                TableCatalog::new_with_indexes(table_name, columns, indexes).ok()
            }) {
                option = self
                    .cache
                    .get_or_insert(table_name.to_string(), |_| Ok(catalog))
                    .ok();
            }
        }

        option
    }

    fn show_tables(&self) -> Result<Vec<String>, StorageError> {
        let mut tables = vec![];
        let (min, max) = TableCodec::root_table_bound();
        let mut iter = self.tx.iter(Bound::Included(&min), Bound::Included(&max))?;

        while let Some((_, value_option)) = iter.try_next().ok().flatten() {
            if let Some(value) = value_option {
                let table_name = TableCodec::decode_root_table(&value)?;

                tables.push(table_name);
            }
        }

        Ok(tables)
    }

    async fn commit(self) -> Result<(), StorageError> {
        self.tx.commit().await?;

        Ok(())
    }
}

impl KipTransaction {
    fn val_to_key(
        table_name: &String,
        index_meta: &IndexMetaRef,
        val: ValueRef,
    ) -> Result<Vec<u8>, TypeError> {
        let index = Index::new(index_meta.id, vec![val]);

        TableCodec::encode_index_key(table_name, &index)
    }

    fn scope_to_iter(
        &self,
        table_name: &String,
        index_meta: &IndexMetaRef,
        min: Bound<ValueRef>,
        max: Bound<ValueRef>,
    ) -> Result<TransactionIter, StorageError> {
        let bound_encode = |bound: Bound<ValueRef>| -> Result<_, StorageError> {
            match bound {
                Bound::Included(val) => Ok(Bound::Included(Self::val_to_key(
                    table_name,
                    &index_meta,
                    val,
                )?)),
                Bound::Excluded(val) => Ok(Bound::Excluded(Self::val_to_key(
                    table_name,
                    &index_meta,
                    val,
                )?)),
                Bound::Unbounded => Ok(Bound::Unbounded),
            }
        };
        let check_bound = |value: &mut Bound<Vec<u8>>, bound: Vec<u8>| {
            if matches!(value, Bound::Unbounded) {
                let _ = mem::replace(value, Bound::Included(bound));
            }
        };
        let (bound_min, bound_max) = TableCodec::index_bound(table_name, &index_meta.id);

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

    fn _drop_data(tx: &mut mvcc::Transaction, min: &[u8], max: &[u8]) -> Result<(), StorageError> {
        let mut iter = tx.iter(Bound::Included(&min), Bound::Included(&max))?;
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
            .filter(|col| col.desc.is_unique)
        {
            if let Some(col_id) = col.id() {
                let meta = IndexMeta {
                    id: 0,
                    column_ids: vec![col_id],
                    name: format!("uk_{}", col.name()),
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
        let _ = transaction.create_table(Arc::new("test".to_string()), source_columns)?;

        let table_catalog = transaction.table(&"test".to_string());
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
            &"test".to_string(),
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
    async fn test_index_iter() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kipsql = Database::with_kipdb(temp_dir.path()).await?;

        let _ = kipsql.run("create table t1 (a int primary key)").await?;
        let _ = kipsql
            .run("insert into t1 (a) values (0), (1), (2)")
            .await?;
        let transaction = kipsql.storage.transaction().await?;

        let table = transaction.table(&"t1".to_string()).unwrap().clone();
        let projections = table
            .all_columns()
            .into_iter()
            .map(|col| ScalarExpression::ColumnRef(col))
            .collect_vec();
        let tuple_ids = vec![
            Arc::new(DataValue::Int32(Some(0))),
            Arc::new(DataValue::Int32(Some(1))),
            Arc::new(DataValue::Int32(Some(2))),
        ];
        let mut iter = IndexIter {
            projections,
            table: &table,
            tuple_ids: VecDeque::from(tuple_ids.clone()),
            tx: &transaction.tx,
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

        let table = transaction.table(&"t1".to_string()).unwrap().clone();
        let projections = table
            .all_columns()
            .into_iter()
            .map(|col| ScalarExpression::ColumnRef(col))
            .collect_vec();
        let mut iter = transaction
            .read_by_index(
                &"t1".to_string(),
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

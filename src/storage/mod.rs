pub mod kipdb;
mod table_codec;

use crate::catalog::{ColumnCatalog, ColumnRef, TableCatalog, TableMeta, TableName};
use crate::errors::DatabaseError;
use crate::expression::range_detacher::Range;
use crate::optimizer::core::statistics_meta::{StatisticMetaLoader, StatisticsMeta};
use crate::storage::table_codec::TableCodec;
use crate::types::index::{Index, IndexId, IndexMetaRef, IndexType};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::{DataValue, ValueRef};
use crate::types::{ColumnId, LogicalType};
use bytes::Bytes;
use itertools::Itertools;
use kip_db::kernel::utils::lru_cache::ShardingLruCache;
use std::collections::{Bound, VecDeque};
use std::ops::SubAssign;
use std::sync::Arc;
use std::{mem, slice};

pub(crate) type StatisticsMetaCache = ShardingLruCache<(TableName, IndexId), StatisticsMeta>;

pub trait Storage: Sync + Send + Clone + 'static {
    type TransactionType: Transaction;

    #[allow(async_fn_in_trait)]
    async fn transaction(&self) -> Result<Self::TransactionType, DatabaseError>;
}

/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);

pub trait Transaction: Sync + Send + 'static + Sized {
    type IterType<'a>: InnerIter;

    /// The bounds is applied to the whole data batches, not per batch.
    ///
    /// The projections is column indices.
    fn read(
        &self,
        table_name: TableName,
        bounds: Bounds,
        mut columns: Vec<(usize, ColumnRef)>,
    ) -> Result<TupleIter<'_, Self>, DatabaseError> {
        assert!(columns.is_sorted_by_key(|(i, _)| i));
        assert!(columns.iter().map(|(i, _)| i).all_unique());

        let table = self
            .table(table_name.clone())
            .ok_or(DatabaseError::TableNotFound)?;
        let table_types = table.types();
        if columns.is_empty() {
            let (i, column) = table.primary_key()?;
            columns.push((i, column.clone()));
        }
        let mut tuple_columns = Vec::with_capacity(columns.len());
        let mut projections = Vec::with_capacity(columns.len());
        for (projection, column) in columns {
            tuple_columns.push(column);
            projections.push(projection);
        }

        let (min, max) = TableCodec::tuple_bound(&table_name);
        let iter = self.range(Bound::Included(&min), Bound::Included(&max))?;

        Ok(TupleIter {
            offset: bounds.0.unwrap_or(0),
            limit: bounds.1,
            table_types,
            tuple_columns: Arc::new(tuple_columns),
            projections,
            iter,
        })
    }

    fn read_by_index(
        &self,
        table_name: TableName,
        (offset_option, limit_option): Bounds,
        columns: Vec<(usize, ColumnRef)>,
        index_meta: IndexMetaRef,
        ranges: Vec<Range>,
    ) -> Result<IndexIter<'_, Self>, DatabaseError> {
        assert!(columns.is_sorted_by_key(|(i, _)| i));
        assert!(columns.iter().map(|(i, _)| i).all_unique());

        let table = self
            .table(table_name.clone())
            .ok_or(DatabaseError::TableNotFound)?;
        let table_types = table.types();
        let table_name = table.name.as_str();
        let offset = offset_option.unwrap_or(0);

        let mut tuple_columns = Vec::with_capacity(columns.len());
        let mut projections = Vec::with_capacity(columns.len());
        for (projection, column) in columns {
            tuple_columns.push(column);
            projections.push(projection);
        }
        let inner = IndexImplEnum::instance(index_meta.ty);

        Ok(IndexIter {
            offset,
            limit: limit_option,
            params: IndexImplParams {
                tuple_schema_ref: Arc::new(tuple_columns),
                projections,
                index_meta,
                table_name,
                table_types,
                tx: self,
            },
            inner,
            ranges: VecDeque::from(ranges),
            scope_iter: None,
        })
    }

    fn add_index_meta(
        &mut self,
        table_name: &TableName,
        index_name: String,
        column_ids: Vec<ColumnId>,
        ty: IndexType,
    ) -> Result<IndexId, DatabaseError> {
        if let Some(mut table) = self.table(table_name.clone()).cloned() {
            let index_meta = table.add_index_meta(index_name, column_ids, ty)?;
            let (key, value) = TableCodec::encode_index_meta(table_name, index_meta)?;
            self.set(key, value)?;
            self.table_cache().remove(table_name);

            Ok(index_meta.id)
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }

    fn add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError> {
        if matches!(index.ty, IndexType::PrimaryKey) {
            return Ok(());
        }
        let (key, value) = TableCodec::encode_index(table_name, &index, tuple_id)?;

        if matches!(index.ty, IndexType::Unique) {
            if let Some(bytes) = self.get(&key)? {
                return if bytes != value {
                    Err(DatabaseError::DuplicateUniqueValue)
                } else {
                    Ok(())
                };
            }
        }
        self.set(key, value)?;

        Ok(())
    }

    fn del_index(
        &mut self,
        table_name: &str,
        index: &Index,
        tuple_id: Option<&TupleId>,
    ) -> Result<(), DatabaseError> {
        if matches!(index.ty, IndexType::PrimaryKey) {
            return Ok(());
        }
        self.remove(&TableCodec::encode_index_key(table_name, index, tuple_id)?)?;

        Ok(())
    }

    fn append(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        types: &[LogicalType],
        is_overwrite: bool,
    ) -> Result<(), DatabaseError> {
        let (key, value) = TableCodec::encode_tuple(table_name, &tuple, types)?;

        if !is_overwrite && self.get(&key)?.is_some() {
            return Err(DatabaseError::DuplicatePrimaryKey);
        }
        self.set(key, value)?;

        Ok(())
    }

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<(), DatabaseError> {
        let key = TableCodec::encode_tuple_key(table_name, &tuple_id)?;
        self.remove(&key)?;

        Ok(())
    }

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId, DatabaseError> {
        if let Some(mut table) = self.table(table_name.clone()).cloned() {
            if !column.nullable && column.default_value()?.is_none() {
                return Err(DatabaseError::NeedNullAbleOrDefault);
            }

            for col in table.columns() {
                if col.name() == column.name() {
                    return if if_not_exists {
                        Ok(col.id().unwrap())
                    } else {
                        Err(DatabaseError::DuplicateColumn(column.name().to_string()))
                    };
                }
            }
            let col_id = table.add_column(column.clone())?;

            if column.desc.is_unique {
                let meta_ref = table.add_index_meta(
                    format!("uk_{}", column.name()),
                    vec![col_id],
                    IndexType::Unique,
                )?;
                let (key, value) = TableCodec::encode_index_meta(table_name, meta_ref)?;
                self.set(key, value)?;
            }

            let column = table.get_column_by_id(&col_id).unwrap();
            let (key, value) = TableCodec::encode_column(table_name, column)?;
            self.set(key, value)?;
            self.table_cache().remove(table_name);

            Ok(col_id)
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }

    fn drop_column(
        &mut self,
        table_name: &TableName,
        column_name: &str,
    ) -> Result<(), DatabaseError> {
        if let Some(table_catalog) = self.table(table_name.clone()).cloned() {
            let column = table_catalog.get_column_by_name(column_name).unwrap();

            let (key, _) = TableCodec::encode_column(table_name, column)?;
            self.remove(&key)?;

            for index_meta in table_catalog.indexes.iter() {
                if !index_meta.column_ids.contains(&column.id().unwrap()) {
                    continue;
                }
                let (index_meta_key, _) = TableCodec::encode_index_meta(table_name, index_meta)?;
                self.remove(&index_meta_key)?;

                let (index_min, index_max) = TableCodec::index_bound(table_name, &index_meta.id);
                self._drop_data(&index_min, &index_max)?;
            }
            self.table_cache().remove(table_name);

            Ok(())
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName, DatabaseError> {
        let (table_key, value) =
            TableCodec::encode_root_table(&TableMeta::empty(table_name.clone()))?;
        if self.get(&table_key)?.is_some() {
            if if_not_exists {
                return Ok(table_name);
            }
            return Err(DatabaseError::TableExists);
        }
        self.set(table_key, value)?;

        let mut table_catalog = TableCatalog::new(table_name.clone(), columns)?;

        self.create_index_meta_for_table(&mut table_catalog)?;

        for column in table_catalog.columns() {
            let (key, value) = TableCodec::encode_column(&table_name, column)?;
            self.set(key, value)?;
        }
        self.table_cache()
            .put(table_name.to_string(), table_catalog);

        Ok(table_name)
    }

    fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), DatabaseError> {
        if self.table(Arc::new(table_name.to_string())).is_none() {
            if if_exists {
                return Ok(());
            } else {
                return Err(DatabaseError::TableNotFound);
            }
        }
        self.drop_data(table_name)?;

        let (column_min, column_max) = TableCodec::columns_bound(table_name);
        self._drop_data(&column_min, &column_max)?;

        let (index_meta_min, index_meta_max) = TableCodec::index_meta_bound(table_name);
        self._drop_data(&index_meta_min, &index_meta_max)?;

        self.remove(&TableCodec::encode_root_table_key(table_name))?;
        self.table_cache().remove(&table_name.to_string());

        Ok(())
    }

    fn drop_data(&mut self, table_name: &str) -> Result<(), DatabaseError> {
        let (tuple_min, tuple_max) = TableCodec::tuple_bound(table_name);
        self._drop_data(&tuple_min, &tuple_max)?;

        let (index_min, index_max) = TableCodec::all_index_bound(table_name);
        self._drop_data(&index_min, &index_max)?;

        Ok(())
    }

    fn table(&self, table_name: TableName) -> Option<&TableCatalog> {
        let mut option = self.table_cache().get(&table_name);

        if option.is_none() {
            // TODO: unify the data into a `Meta` prefix and use one iteration to collect all data
            let (columns, indexes) = self.table_collect(table_name.clone()).ok()?;

            if let Ok(catalog) = TableCatalog::reload(table_name.clone(), columns, indexes) {
                option = self
                    .table_cache()
                    .get_or_insert(table_name.to_string(), |_| Ok(catalog))
                    .ok();
            }
        }

        option
    }

    fn table_metas(&self) -> Result<Vec<TableMeta>, DatabaseError> {
        let mut metas = vec![];
        let (min, max) = TableCodec::root_table_bound();
        let mut iter = self.range(Bound::Included(&min), Bound::Included(&max))?;

        while let Some((_, value)) = iter.try_next().ok().flatten() {
            let meta = TableCodec::decode_root_table(&value)?;

            metas.push(meta);
        }

        Ok(metas)
    }

    fn save_table_meta(
        &mut self,
        table_name: &TableName,
        path: String,
        statistics_meta: StatisticsMeta,
    ) -> Result<(), DatabaseError> {
        // TODO: clean old meta file
        let index_id = statistics_meta.index_id();
        let _ = self
            .meta_cache()
            .put((table_name.clone(), index_id), statistics_meta);
        let (key, value) = TableCodec::encode_statistics_path(table_name.as_str(), index_id, path);

        self.set(key, value)?;

        Ok(())
    }

    fn table_meta_path(
        &self,
        table_name: &str,
        index_id: IndexId,
    ) -> Result<Option<String>, DatabaseError> {
        let key = TableCodec::encode_statistics_path_key(table_name, index_id);
        self.get(&key)?
            .map(|bytes| TableCodec::decode_statistics_path(&bytes))
            .transpose()
    }

    fn meta_loader(&self) -> StatisticMetaLoader<Self>
    where
        Self: Sized,
    {
        StatisticMetaLoader::new(self, self.meta_cache())
    }

    fn table_collect(
        &self,
        table_name: TableName,
    ) -> Result<(Vec<ColumnCatalog>, Vec<IndexMetaRef>), DatabaseError> {
        let (table_min, table_max) = TableCodec::table_bound(&table_name);
        let mut column_iter =
            self.range(Bound::Included(&table_min), Bound::Included(&table_max))?;

        let mut columns = Vec::new();
        let mut index_metas = Vec::new();

        // Tips: only `Column`, `IndexMeta`, `TableMeta`
        while let Some((key, value)) = column_iter.try_next().ok().flatten() {
            if key.starts_with(&table_min) {
                columns.push(TableCodec::decode_column(&value)?);
            } else {
                index_metas.push(Arc::new(TableCodec::decode_index_meta(&value)?));
            }
        }

        Ok((columns, index_metas))
    }

    fn _drop_data(&mut self, min: &[u8], max: &[u8]) -> Result<(), DatabaseError> {
        let mut iter = self.range(Bound::Included(min), Bound::Included(max))?;
        let mut data_keys = vec![];

        while let Some((key, _)) = iter.try_next()? {
            data_keys.push(key);
        }
        drop(iter);

        for key in data_keys {
            self.remove(&key)?
        }

        Ok(())
    }

    fn create_index_meta_for_table(
        &mut self,
        table: &mut TableCatalog,
    ) -> Result<(), DatabaseError> {
        let table_name = table.name.clone();
        let index_column = table
            .columns()
            .filter(|column| column.desc.is_primary || column.desc.is_unique)
            .map(|column| (column.id().unwrap(), column.clone()))
            .collect_vec();

        for (col_id, col) in index_column {
            let is_primary = col.desc.is_primary;
            let index_ty = if is_primary {
                IndexType::PrimaryKey
            } else if col.desc.is_unique {
                IndexType::Unique
            } else {
                continue;
            };
            // FIXME: composite indexes may exist on future
            let prefix = if is_primary { "pk" } else { "uk" };

            let meta_ref = table.add_index_meta(
                format!("{}_{}", prefix, col.name()),
                vec![col_id],
                index_ty,
            )?;
            let (key, value) = TableCodec::encode_index_meta(&table_name, meta_ref)?;
            self.set(key, value)?;
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, DatabaseError>;

    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), DatabaseError>;

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError>;

    fn range<'a>(
        &'a self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
    ) -> Result<Self::IterType<'a>, DatabaseError>;

    fn table_cache(&self) -> &ShardingLruCache<String, TableCatalog>;
    fn meta_cache(&self) -> &StatisticsMetaCache;

    #[allow(async_fn_in_trait)]
    async fn commit(self) -> Result<(), DatabaseError>;
}

trait IndexImpl<T: Transaction> {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError>;

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError>;

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &ValueRef,
        is_upper: bool,
    ) -> Result<Vec<u8>, DatabaseError>;
}

enum IndexImplEnum {
    PrimaryKey(PrimaryKeyIndexImpl),
    Unique(UniqueIndexImpl),
    Normal(NormalIndexImpl),
    Composite(CompositeIndexImpl),
}

impl IndexImplEnum {
    fn instance(index_type: IndexType) -> IndexImplEnum {
        match index_type {
            IndexType::PrimaryKey => IndexImplEnum::PrimaryKey(PrimaryKeyIndexImpl),
            IndexType::Unique => IndexImplEnum::Unique(UniqueIndexImpl),
            IndexType::Normal => IndexImplEnum::Normal(NormalIndexImpl),
            IndexType::Composite => IndexImplEnum::Composite(CompositeIndexImpl),
        }
    }
}

struct PrimaryKeyIndexImpl;
struct UniqueIndexImpl;
struct NormalIndexImpl;
struct CompositeIndexImpl;

struct IndexImplParams<'a, T: Transaction> {
    tuple_schema_ref: Arc<Vec<ColumnRef>>,
    projections: Vec<usize>,

    index_meta: IndexMetaRef,
    table_name: &'a str,
    table_types: Vec<LogicalType>,
    tx: &'a T,
}

impl<T: Transaction> IndexImplParams<'_, T> {
    fn get_tuple_by_id(&self, tuple_id: &TupleId) -> Result<Option<Tuple>, DatabaseError> {
        let key = TableCodec::encode_tuple_key(self.table_name, tuple_id)?;

        Ok(self.tx.get(&key)?.map(|bytes| {
            TableCodec::decode_tuple(
                &self.table_types,
                &self.projections,
                &self.tuple_schema_ref,
                &bytes,
            )
        }))
    }
}

enum IndexResult<'a, T: Transaction> {
    Tuple(Tuple),
    Scope(T::IterType<'a>),
}

impl<T: Transaction> IndexImpl<T> for IndexImplEnum {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => inner.index_lookup(bytes, params),
            IndexImplEnum::Unique(inner) => inner.index_lookup(bytes, params),
            IndexImplEnum::Normal(inner) => inner.index_lookup(bytes, params),
            IndexImplEnum::Composite(inner) => inner.index_lookup(bytes, params),
        }
    }

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => inner.eq_to_res(value, params),
            IndexImplEnum::Unique(inner) => inner.eq_to_res(value, params),
            IndexImplEnum::Normal(inner) => inner.eq_to_res(value, params),
            IndexImplEnum::Composite(inner) => inner.eq_to_res(value, params),
        }
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &ValueRef,
        is_upper: bool,
    ) -> Result<Vec<u8>, DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => inner.bound_key(params, value, is_upper),
            IndexImplEnum::Unique(inner) => inner.bound_key(params, value, is_upper),
            IndexImplEnum::Normal(inner) => inner.bound_key(params, value, is_upper),
            IndexImplEnum::Composite(inner) => inner.bound_key(params, value, is_upper),
        }
    }
}

impl<T: Transaction> IndexImpl<T> for PrimaryKeyIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        Ok(TableCodec::decode_tuple(
            &params.table_types,
            &params.projections,
            &params.tuple_schema_ref,
            bytes,
        ))
    }

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let bytes = params
            .tx
            .get(&TableCodec::encode_tuple_key(params.table_name, value)?)?
            .ok_or_else(|| {
                DatabaseError::NotFound("secondary index", format!("tuple_id -> {}", value))
            })?;
        let tuple = TableCodec::decode_tuple(
            &params.table_types,
            &params.projections,
            &params.tuple_schema_ref,
            &bytes,
        );
        Ok(IndexResult::Tuple(tuple))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        val: &ValueRef,
        _: bool,
    ) -> Result<Vec<u8>, DatabaseError> {
        TableCodec::encode_tuple_key(params.table_name, val)
    }
}

fn secondary_index_lookup<T: Transaction>(
    bytes: &Bytes,
    params: &IndexImplParams<T>,
) -> Result<Tuple, DatabaseError> {
    let tuple_id = TableCodec::decode_index(bytes, &params.index_meta.pk_ty);
    params
        .get_tuple_by_id(&tuple_id)?
        .ok_or_else(|| DatabaseError::NotFound("index's tuple_id", tuple_id.to_string()))
}

impl<T: Transaction> IndexImpl<T> for UniqueIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        secondary_index_lookup(bytes, params)
    }

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let bytes = params
            .tx
            .get(&self.bound_key(params, value, false)?)?
            .ok_or_else(|| {
                DatabaseError::NotFound("secondary index", format!("index_value -> {}", value))
            })?;
        let tuple_id = TableCodec::decode_index(&bytes, &params.index_meta.pk_ty);
        let tuple = params.get_tuple_by_id(&tuple_id)?.ok_or_else(|| {
            DatabaseError::NotFound("secondary index", format!("tuple_id -> {}", value))
        })?;
        Ok(IndexResult::Tuple(tuple))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &ValueRef,
        _: bool,
    ) -> Result<Vec<u8>, DatabaseError> {
        let index = Index::new(
            params.index_meta.id,
            slice::from_ref(value),
            IndexType::Unique,
        );

        TableCodec::encode_index_key(params.table_name, &index, None)
    }
}

impl<T: Transaction> IndexImpl<T> for NormalIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        secondary_index_lookup(bytes, params)
    }

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let min = self.bound_key(params, value, false)?;
        let max = self.bound_key(params, value, true)?;

        let iter = params.tx.range(
            Bound::Included(min.as_slice()),
            Bound::Included(max.as_slice()),
        )?;
        Ok(IndexResult::Scope(iter))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &ValueRef,
        is_upper: bool,
    ) -> Result<Vec<u8>, DatabaseError> {
        let index = Index::new(
            params.index_meta.id,
            slice::from_ref(value),
            IndexType::Normal,
        );

        TableCodec::encode_index_bound_key(params.table_name, &index, is_upper)
    }
}

impl<T: Transaction> IndexImpl<T> for CompositeIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        secondary_index_lookup(bytes, params)
    }

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let min = self.bound_key(params, value, false)?;
        let max = self.bound_key(params, value, true)?;

        let iter = params.tx.range(
            Bound::Included(min.as_slice()),
            Bound::Included(max.as_slice()),
        )?;
        Ok(IndexResult::Scope(iter))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &ValueRef,
        is_upper: bool,
    ) -> Result<Vec<u8>, DatabaseError> {
        let values = if let DataValue::Tuple(Some(values)) = value.as_ref() {
            values.as_slice()
        } else {
            slice::from_ref(value)
        };
        let index = Index::new(params.index_meta.id, values, IndexType::Composite);

        TableCodec::encode_index_bound_key(params.table_name, &index, is_upper)
    }
}

pub struct TupleIter<'a, T: Transaction> {
    offset: usize,
    limit: Option<usize>,
    table_types: Vec<LogicalType>,
    tuple_columns: Arc<Vec<ColumnRef>>,
    projections: Vec<usize>,
    iter: T::IterType<'a>,
}

impl<T: Transaction> Iter for TupleIter<'_, T> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        while self.offset > 0 {
            let _ = self.iter.try_next()?;
            self.offset -= 1;
        }

        if let Some(num) = self.limit {
            if num == 0 {
                return Ok(None);
            }
        }

        #[allow(clippy::never_loop)]
        while let Some((_, value)) = self.iter.try_next()? {
            let tuple = TableCodec::decode_tuple(
                &self.table_types,
                &self.projections,
                &self.tuple_columns,
                &value,
            );

            if let Some(num) = self.limit.as_mut() {
                num.sub_assign(1);
            }

            return Ok(Some(tuple));
        }

        Ok(None)
    }
}

pub struct IndexIter<'a, T: Transaction> {
    offset: usize,
    limit: Option<usize>,

    params: IndexImplParams<'a, T>,
    inner: IndexImplEnum,
    // for buffering data
    ranges: VecDeque<Range>,
    scope_iter: Option<T::IterType<'a>>,
}

impl<T: Transaction> IndexIter<'_, T> {
    fn offset_move(offset: &mut usize) -> bool {
        if *offset > 0 {
            offset.sub_assign(1);

            true
        } else {
            false
        }
    }

    fn limit_sub(limit: &mut Option<usize>) {
        if let Some(num) = limit.as_mut() {
            num.sub_assign(1);
        }
    }

    fn is_empty(&self) -> bool {
        self.scope_iter.is_none() && self.ranges.is_empty()
    }
}

/// expression -> index value -> tuple
impl<T: Transaction> Iter for IndexIter<'_, T> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if matches!(self.limit, Some(0)) || self.is_empty() {
            self.scope_iter = None;
            self.ranges.clear();

            return Ok(None);
        }

        if let Some(iter) = &mut self.scope_iter {
            while let Some((_, bytes)) = iter.try_next()? {
                if Self::offset_move(&mut self.offset) {
                    continue;
                }
                Self::limit_sub(&mut self.limit);
                let tuple = self.inner.index_lookup(&bytes, &self.params)?;

                return Ok(Some(tuple));
            }
            self.scope_iter = None;
        }

        if let Some(binary) = self.ranges.pop_front() {
            match binary {
                Range::Scope { min, max } => {
                    let table_name = self.params.table_name;
                    let index_meta = &self.params.index_meta;
                    let bound_encode =
                        |bound: Bound<ValueRef>, is_upper: bool| -> Result<_, DatabaseError> {
                            match bound {
                                Bound::Included(val) => Ok(Bound::Included(self.inner.bound_key(
                                    &self.params,
                                    &val,
                                    is_upper,
                                )?)),
                                Bound::Excluded(val) => Ok(Bound::Excluded(self.inner.bound_key(
                                    &self.params,
                                    &val,
                                    is_upper,
                                )?)),
                                Bound::Unbounded => Ok(Bound::Unbounded),
                            }
                        };
                    let (bound_min, bound_max) = if matches!(index_meta.ty, IndexType::PrimaryKey) {
                        TableCodec::tuple_bound(table_name)
                    } else {
                        TableCodec::index_bound(table_name, &index_meta.id)
                    };
                    let check_bound = |value: &mut Bound<Vec<u8>>, bound: Vec<u8>| {
                        if matches!(value, Bound::Unbounded) {
                            let _ = mem::replace(value, Bound::Included(bound));
                        }
                    };

                    let mut encode_min = bound_encode(min, false)?;
                    check_bound(&mut encode_min, bound_min);

                    let mut encode_max = bound_encode(max, true)?;
                    check_bound(&mut encode_max, bound_max);

                    let iter = self.params.tx.range(
                        encode_min.as_ref().map(Vec::as_slice),
                        encode_max.as_ref().map(Vec::as_slice),
                    )?;
                    self.scope_iter = Some(iter);
                }
                Range::Eq(val) => match self.inner.eq_to_res(&val, &self.params)? {
                    IndexResult::Tuple(tuple) => {
                        if Self::offset_move(&mut self.offset) {
                            return self.next_tuple();
                        }
                        Self::limit_sub(&mut self.limit);
                        return Ok(Some(tuple));
                    }
                    IndexResult::Scope(iter) => self.scope_iter = Some(iter),
                },
                _ => (),
            }
        }
        self.next_tuple()
    }
}

pub trait InnerIter: Sync + Send {
    fn try_next(&mut self) -> Result<Option<(Bytes, Bytes)>, DatabaseError>;
}

pub trait Iter: Sync + Send {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError>;
}

pub mod kip;
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
use kip_db::kernel::lsm::iterator::Iter as DBIter;
use kip_db::kernel::lsm::mvcc;
use std::collections::{Bound, VecDeque};
use std::ops::SubAssign;
use std::sync::Arc;
use std::{mem, slice};

pub trait Storage: Sync + Send + Clone + 'static {
    type TransactionType: Transaction;

    #[allow(async_fn_in_trait)]
    async fn transaction(&self) -> Result<Self::TransactionType, DatabaseError>;
}

/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);

pub trait Transaction: Sync + Send + 'static {
    type IterType<'a>: Iter;

    /// The bounds is applied to the whole data batches, not per batch.
    ///
    /// The projections is column indices.
    fn read(
        &self,
        table_name: TableName,
        bounds: Bounds,
        columns: Vec<(usize, ColumnRef)>,
    ) -> Result<Self::IterType<'_>, DatabaseError>;

    fn read_by_index(
        &self,
        table_name: TableName,
        bounds: Bounds,
        columns: Vec<(usize, ColumnRef)>,
        index_meta: IndexMetaRef,
        ranges: Vec<Range>,
    ) -> Result<IndexIter<'_>, DatabaseError>;

    fn add_index_meta(
        &mut self,
        table_name: &TableName,
        index_name: String,
        column_ids: Vec<ColumnId>,
        ty: IndexType,
    ) -> Result<IndexId, DatabaseError>;

    fn add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError>;

    fn del_index(
        &mut self,
        table_name: &str,
        index: &Index,
        tuple_id: Option<&TupleId>,
    ) -> Result<(), DatabaseError>;

    fn append(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        types: &[LogicalType],
        is_overwrite: bool,
    ) -> Result<(), DatabaseError>;

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<(), DatabaseError>;

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId, DatabaseError>;

    fn drop_column(&mut self, table_name: &TableName, column: &str) -> Result<(), DatabaseError>;

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName, DatabaseError>;

    fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), DatabaseError>;
    fn drop_data(&mut self, table_name: &str) -> Result<(), DatabaseError>;
    fn table(&self, table_name: TableName) -> Option<&TableCatalog>;
    fn table_metas(&self) -> Result<Vec<TableMeta>, DatabaseError>;
    fn save_table_meta(
        &mut self,
        table_name: &TableName,
        path: String,
        statistics_meta: StatisticsMeta,
    ) -> Result<(), DatabaseError>;
    fn table_meta_path(
        &self,
        table_name: &str,
        index_id: IndexId,
    ) -> Result<Option<String>, DatabaseError>;
    fn meta_loader(&self) -> StatisticMetaLoader<Self>
    where
        Self: Sized;

    #[allow(async_fn_in_trait)]
    async fn commit(self) -> Result<(), DatabaseError>;
}

trait IndexImpl {
    fn index_lookup(&self, bytes: &Bytes, params: &IndexImplParams)
        -> Result<Tuple, DatabaseError>;

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a>,
    ) -> Result<IndexResult<'a>, DatabaseError>;

    fn bound_key(
        &self,
        params: &IndexImplParams,
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

struct IndexImplParams<'a> {
    tuple_schema_ref: Arc<Vec<ColumnRef>>,
    projections: Vec<usize>,

    index_meta: IndexMetaRef,
    table_name: &'a str,
    table_types: Vec<LogicalType>,
    tx: &'a mvcc::Transaction,
}

impl IndexImplParams<'_> {
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

enum IndexResult<'a> {
    Tuple(Tuple),
    Scope(mvcc::TransactionIter<'a>),
}

impl IndexImpl for IndexImplEnum {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams,
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
        params: &IndexImplParams<'a>,
    ) -> Result<IndexResult<'a>, DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => inner.eq_to_res(value, params),
            IndexImplEnum::Unique(inner) => inner.eq_to_res(value, params),
            IndexImplEnum::Normal(inner) => inner.eq_to_res(value, params),
            IndexImplEnum::Composite(inner) => inner.eq_to_res(value, params),
        }
    }

    fn bound_key(
        &self,
        params: &IndexImplParams,
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

impl IndexImpl for PrimaryKeyIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams,
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
        params: &IndexImplParams<'a>,
    ) -> Result<IndexResult<'a>, DatabaseError> {
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
        params: &IndexImplParams,
        val: &ValueRef,
        _: bool,
    ) -> Result<Vec<u8>, DatabaseError> {
        TableCodec::encode_tuple_key(params.table_name, val)
    }
}

fn secondary_index_lookup(bytes: &Bytes, params: &IndexImplParams) -> Result<Tuple, DatabaseError> {
    let tuple_id = TableCodec::decode_index(bytes, &params.index_meta.pk_ty);
    params
        .get_tuple_by_id(&tuple_id)?
        .ok_or_else(|| DatabaseError::NotFound("index's tuple_id", tuple_id.to_string()))
}

impl IndexImpl for UniqueIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams,
    ) -> Result<Tuple, DatabaseError> {
        secondary_index_lookup(bytes, params)
    }

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a>,
    ) -> Result<IndexResult<'a>, DatabaseError> {
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
        params: &IndexImplParams,
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

impl IndexImpl for NormalIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams,
    ) -> Result<Tuple, DatabaseError> {
        secondary_index_lookup(bytes, params)
    }

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a>,
    ) -> Result<IndexResult<'a>, DatabaseError> {
        let min = self.bound_key(params, value, false)?;
        let max = self.bound_key(params, value, true)?;

        let iter = params.tx.iter(
            Bound::Included(min.as_slice()),
            Bound::Included(max.as_slice()),
        )?;
        Ok(IndexResult::Scope(iter))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams,
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

impl IndexImpl for CompositeIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        params: &IndexImplParams,
    ) -> Result<Tuple, DatabaseError> {
        secondary_index_lookup(bytes, params)
    }

    fn eq_to_res<'a>(
        &self,
        value: &ValueRef,
        params: &IndexImplParams<'a>,
    ) -> Result<IndexResult<'a>, DatabaseError> {
        let min = self.bound_key(params, value, false)?;
        let max = self.bound_key(params, value, true)?;

        let iter = params.tx.iter(
            Bound::Included(min.as_slice()),
            Bound::Included(max.as_slice()),
        )?;
        Ok(IndexResult::Scope(iter))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams,
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

// TODO: Table return optimization
pub struct IndexIter<'a> {
    offset: usize,
    limit: Option<usize>,

    params: IndexImplParams<'a>,
    inner: IndexImplEnum,
    // for buffering data
    ranges: VecDeque<Range>,
    scope_iter: Option<mvcc::TransactionIter<'a>>,
}

impl IndexIter<'_> {
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
impl Iter for IndexIter<'_> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if matches!(self.limit, Some(0)) || self.is_empty() {
            self.scope_iter = None;
            self.ranges.clear();

            return Ok(None);
        }

        if let Some(iter) = &mut self.scope_iter {
            while let Some((_, value_option)) = iter.try_next()? {
                if let Some(bytes) = value_option {
                    if Self::offset_move(&mut self.offset) {
                        continue;
                    }
                    Self::limit_sub(&mut self.limit);
                    let tuple = self.inner.index_lookup(&bytes, &self.params)?;

                    return Ok(Some(tuple));
                }
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

                    let iter = self.params.tx.iter(
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

pub trait Iter: Sync + Send {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError>;
}

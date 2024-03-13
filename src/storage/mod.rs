pub mod kip;
mod table_codec;

use crate::catalog::{ColumnCatalog, ColumnRef, TableCatalog, TableMeta, TableName};
use crate::errors::DatabaseError;
use crate::expression::range_detacher::Range;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::storage::table_codec::TableCodec;
use crate::types::index::{Index, IndexId, IndexMetaRef, IndexType};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::{DataValue, ValueRef};
use crate::types::ColumnId;
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
    fn save_table_meta(&mut self, table_meta: &TableMeta) -> Result<(), DatabaseError>;
    fn statistics_meta_paths(&self, table_name: &str) -> Result<Vec<String>, DatabaseError>;
    fn meta_loader(&self) -> StatisticMetaLoader<Self>
    where
        Self: Sized;

    #[allow(async_fn_in_trait)]
    async fn commit(self) -> Result<(), DatabaseError>;
}

#[derive(Debug)]
enum IndexValue {
    PrimaryKey(Tuple),
    Normal(TupleId),
}

// TODO: Table return optimization
pub struct IndexIter<'a> {
    offset: usize,
    limit: Option<usize>,
    tuple_schema_ref: Arc<Vec<ColumnRef>>,
    projections: Vec<usize>,

    index_meta: IndexMetaRef,
    table: &'a TableCatalog,
    tx: &'a mvcc::Transaction,

    // for buffering data
    index_values: VecDeque<IndexValue>,
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

    fn bound_key(&self, val: &ValueRef, is_upper: bool) -> Result<Vec<u8>, DatabaseError> {
        match self.index_meta.ty {
            IndexType::PrimaryKey => TableCodec::encode_tuple_key(&self.table.name, val),
            IndexType::Unique => {
                let index =
                    Index::new(self.index_meta.id, slice::from_ref(val), self.index_meta.ty);

                TableCodec::encode_index_key(&self.table.name, &index, None)
            }
            IndexType::Normal => {
                let index =
                    Index::new(self.index_meta.id, slice::from_ref(val), self.index_meta.ty);

                TableCodec::encode_index_bound_key(&self.table.name, &index, is_upper)
            }
            IndexType::Composite => {
                let values = if let DataValue::Tuple(Some(values)) = val.as_ref() {
                    values.as_slice()
                } else {
                    slice::from_ref(val)
                };
                let index = Index::new(self.index_meta.id, values, self.index_meta.ty);

                TableCodec::encode_index_bound_key(&self.table.name, &index, is_upper)
            }
        }
    }

    fn get_tuple_by_id(&mut self, tuple_id: &TupleId) -> Result<Option<Tuple>, DatabaseError> {
        let key = TableCodec::encode_tuple_key(&self.table.name, tuple_id)?;

        Ok(self.tx.get(&key)?.map(|bytes| {
            TableCodec::decode_tuple(
                &self.table.types(),
                &self.projections,
                &self.tuple_schema_ref,
                &bytes,
            )
        }))
    }

    fn is_empty(&self) -> bool {
        self.scope_iter.is_none() && self.index_values.is_empty() && self.ranges.is_empty()
    }
}

/// expression -> index value -> tuple
impl Iter for IndexIter<'_> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        // 1. check limit
        if matches!(self.limit, Some(0)) || self.is_empty() {
            self.scope_iter = None;
            self.ranges.clear();

            return Ok(None);
        }
        // 2. try get tuple on index_values and until it empty
        while let Some(value) = self.index_values.pop_front() {
            if Self::offset_move(&mut self.offset) {
                continue;
            }
            let tuple = match value {
                IndexValue::PrimaryKey(tuple) => {
                    if let Some(num) = self.limit.as_mut() {
                        num.sub_assign(1);
                    }

                    return Ok(Some(tuple));
                }
                IndexValue::Normal(tuple_id) => {
                    self.get_tuple_by_id(&tuple_id)?.ok_or_else(|| {
                        DatabaseError::NotFound("index's tuple_id", tuple_id.to_string())
                    })?
                }
            };
            return Ok(Some(tuple));
        }
        assert!(self.index_values.is_empty());

        // 3. If the current expression is a Scope,
        // an iterator will be generated for reading the IndexValues of the Scope.
        if let Some(iter) = &mut self.scope_iter {
            while let Some((_, value_option)) = iter.try_next()? {
                if let Some(value) = value_option {
                    let index = if matches!(self.index_meta.ty, IndexType::PrimaryKey) {
                        let tuple = TableCodec::decode_tuple(
                            &self.table.types(),
                            &self.projections,
                            &self.tuple_schema_ref,
                            &value,
                        );

                        IndexValue::PrimaryKey(tuple)
                    } else {
                        IndexValue::Normal(TableCodec::decode_index(&value, &self.index_meta.pk_ty))
                    };
                    self.index_values.push_back(index);
                    break;
                }
            }
            if self.index_values.is_empty() {
                self.scope_iter = None;
            }
            return self.next_tuple();
        }

        // 4. When `scope_iter` and `index_values` do not have a value, use the next expression to iterate
        if let Some(binary) = self.ranges.pop_front() {
            match binary {
                Range::Scope { min, max } => {
                    let table_name = &self.table.name;
                    let index_meta = &self.index_meta;

                    let bound_encode =
                        |bound: Bound<ValueRef>, is_upper: bool| -> Result<_, DatabaseError> {
                            match bound {
                                Bound::Included(val) => {
                                    Ok(Bound::Included(self.bound_key(&val, is_upper)?))
                                }
                                Bound::Excluded(val) => {
                                    Ok(Bound::Excluded(self.bound_key(&val, is_upper)?))
                                }
                                Bound::Unbounded => Ok(Bound::Unbounded),
                            }
                        };
                    let check_bound = |value: &mut Bound<Vec<u8>>, bound: Vec<u8>| {
                        if matches!(value, Bound::Unbounded) {
                            let _ = mem::replace(value, Bound::Included(bound));
                        }
                    };
                    let (bound_min, bound_max) = if matches!(index_meta.ty, IndexType::PrimaryKey) {
                        TableCodec::tuple_bound(table_name)
                    } else {
                        TableCodec::index_bound(table_name, &index_meta.id)
                    };

                    let mut encode_min = bound_encode(min, false)?;
                    check_bound(&mut encode_min, bound_min);

                    let mut encode_max = bound_encode(max, true)?;
                    check_bound(&mut encode_max, bound_max);

                    let iter = self.tx.iter(
                        encode_min.as_ref().map(Vec::as_slice),
                        encode_max.as_ref().map(Vec::as_slice),
                    )?;
                    self.scope_iter = Some(iter);
                }
                Range::Eq(val) => {
                    match self.index_meta.ty {
                        IndexType::PrimaryKey => {
                            let bytes =
                                self.tx.get(&self.bound_key(&val, false)?)?.ok_or_else(|| {
                                    DatabaseError::NotFound(
                                        "secondary index",
                                        format!("value -> {}", val),
                                    )
                                })?;

                            let tuple = TableCodec::decode_tuple(
                                &self.table.types(),
                                &self.projections,
                                &self.tuple_schema_ref,
                                &bytes,
                            );
                            self.index_values.push_back(IndexValue::PrimaryKey(tuple));
                            self.scope_iter = None;
                        }
                        IndexType::Unique => {
                            let bytes =
                                self.tx.get(&self.bound_key(&val, false)?)?.ok_or_else(|| {
                                    DatabaseError::NotFound(
                                        "secondary index",
                                        format!("value -> {}", val),
                                    )
                                })?;

                            self.index_values.push_back(IndexValue::Normal(
                                TableCodec::decode_index(&bytes, &self.index_meta.pk_ty),
                            ));
                            self.scope_iter = None;
                        }
                        IndexType::Normal | IndexType::Composite => {
                            let min = self.bound_key(&val, false)?;
                            let max = self.bound_key(&val, true)?;

                            let iter = self.tx.iter(
                                Bound::Included(min.as_slice()),
                                Bound::Included(max.as_slice()),
                            )?;
                            self.scope_iter = Some(iter);
                        }
                    }
                }
                _ => (),
            }
        }
        self.next_tuple()
    }
}

pub trait Iter: Sync + Send {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError>;
}

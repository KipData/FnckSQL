pub mod kip;
mod table_codec;

use crate::catalog::{CatalogError, ColumnCatalog, TableCatalog, TableMeta, TableName};
use crate::expression::simplify::ConstantBinary;
use crate::expression::ScalarExpression;
use crate::storage::table_codec::TableCodec;
use crate::types::errors::TypeError;
use crate::types::index::{Index, IndexMetaRef};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::ValueRef;
use crate::types::ColumnId;
use kip_db::kernel::lsm::iterator::Iter as DBIter;
use kip_db::kernel::lsm::mvcc;
use kip_db::KernelError;
use std::collections::{Bound, VecDeque};
use std::mem;
use std::ops::SubAssign;

pub trait Storage: Sync + Send + Clone + 'static {
    type TransactionType: Transaction;

    #[allow(async_fn_in_trait)]
    async fn transaction(&self) -> Result<Self::TransactionType, StorageError>;
}

/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);
type Projections = Vec<ScalarExpression>;

pub trait Transaction: Sync + Send + 'static {
    type IterType<'a>: Iter;

    /// The bounds is applied to the whole data batches, not per batch.
    ///
    /// The projections is column indices.
    fn read(
        &self,
        table_name: TableName,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::IterType<'_>, StorageError>;

    fn read_by_index(
        &self,
        table_name: TableName,
        bounds: Bounds,
        projection: Projections,
        index_meta: IndexMetaRef,
        binaries: Vec<ConstantBinary>,
    ) -> Result<IndexIter<'_>, StorageError>;

    fn add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_ids: Vec<TupleId>,
        is_unique: bool,
    ) -> Result<(), StorageError>;

    fn del_index(&mut self, table_name: &str, index: &Index) -> Result<(), StorageError>;

    fn append(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        is_overwrite: bool,
    ) -> Result<(), StorageError>;

    fn delete(&mut self, table_name: &str, tuple_id: TupleId) -> Result<(), StorageError>;

    fn add_column(
        &mut self,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId, StorageError>;

    fn drop_column(
        &mut self,
        table_name: &TableName,
        column: &str,
        if_exists: bool,
    ) -> Result<(), StorageError>;

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName, StorageError>;

    fn drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<(), StorageError>;
    fn drop_data(&mut self, table_name: &str) -> Result<(), StorageError>;
    fn table(&self, table_name: TableName) -> Option<&TableCatalog>;
    fn table_metas(&self) -> Result<Vec<TableMeta>, StorageError>;
    fn histogram_paths(&self, table_name: &str) -> Result<Vec<String>, StorageError>;

    #[allow(async_fn_in_trait)]
    async fn commit(self) -> Result<(), StorageError>;
}

enum IndexValue {
    PrimaryKey(Tuple),
    Normal(TupleId),
}

// TODO: Table return optimization
pub struct IndexIter<'a> {
    offset: usize,
    limit: Option<usize>,
    projections: Projections,

    index_meta: IndexMetaRef,
    table: &'a TableCatalog,
    tx: &'a mvcc::Transaction,

    // for buffering data
    index_values: VecDeque<IndexValue>,
    binaries: VecDeque<ConstantBinary>,
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

    fn val_to_key(&self, val: ValueRef) -> Result<Vec<u8>, TypeError> {
        if self.index_meta.is_unique {
            let index = Index::new(self.index_meta.id, vec![val]);

            TableCodec::encode_index_key(&self.table.name, &index)
        } else {
            TableCodec::encode_tuple_key(&self.table.name, &val)
        }
    }

    fn get_tuple_by_id(&mut self, tuple_id: &TupleId) -> Result<Option<Tuple>, StorageError> {
        let key = TableCodec::encode_tuple_key(&self.table.name, &tuple_id)?;

        self.tx
            .get(&key)?
            .map(|bytes| {
                let tuple = TableCodec::decode_tuple(self.table.all_columns(), &bytes);

                tuple_projection(&mut self.limit, &self.projections, tuple)
            })
            .transpose()
    }

    fn is_empty(&self) -> bool {
        self.scope_iter.is_none() && self.index_values.is_empty() && self.binaries.is_empty()
    }
}

impl Iter for IndexIter<'_> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError> {
        // 1. check limit
        if matches!(self.limit, Some(0)) || self.is_empty() {
            self.scope_iter = None;
            self.binaries.clear();

            return Ok(None);
        }
        // 2. try get tuple on index_values and until it empty
        loop {
            if let Some(value) = self.index_values.pop_front() {
                if Self::offset_move(&mut self.offset) {
                    continue;
                }
                match value {
                    IndexValue::PrimaryKey(tuple) => {
                        let tuple = tuple_projection(&mut self.limit, &self.projections, tuple)?;

                        return Ok(Some(tuple));
                    }
                    IndexValue::Normal(tuple_id) => {
                        if let Some(tuple) = self.get_tuple_by_id(&tuple_id)? {
                            return Ok(Some(tuple));
                        }
                    }
                }
            } else {
                break;
            }
        }
        assert!(self.index_values.is_empty());

        // 3. If the current expression is a Scope,
        // an iterator will be generated for reading the IndexValues of the Scope.
        if let Some(iter) = &mut self.scope_iter {
            let mut has_next = false;
            while let Some((_, value_option)) = iter.try_next()? {
                if let Some(value) = value_option {
                    if self.index_meta.is_primary {
                        let tuple = TableCodec::decode_tuple(self.table.all_columns(), &value);

                        self.index_values.push_back(IndexValue::PrimaryKey(tuple));
                    } else {
                        for tuple_id in TableCodec::decode_index(&value)? {
                            self.index_values.push_back(IndexValue::Normal(tuple_id));
                        }
                    }
                    has_next = true;
                    break;
                }
            }
            if !has_next {
                self.scope_iter = None;
            }
            return self.next_tuple();
        }

        // 4. When `scope_iter` and `index_values` do not have a value, use the next expression to iterate
        if let Some(binary) = self.binaries.pop_front() {
            match binary {
                ConstantBinary::Scope { min, max } => {
                    let table_name = &self.table.name;
                    let index_meta = &self.index_meta;

                    let bound_encode = |bound: Bound<ValueRef>| -> Result<_, StorageError> {
                        match bound {
                            Bound::Included(val) => Ok(Bound::Included(self.val_to_key(val)?)),
                            Bound::Excluded(val) => Ok(Bound::Excluded(self.val_to_key(val)?)),
                            Bound::Unbounded => Ok(Bound::Unbounded),
                        }
                    };
                    let check_bound = |value: &mut Bound<Vec<u8>>, bound: Vec<u8>| {
                        if matches!(value, Bound::Unbounded) {
                            let _ = mem::replace(value, Bound::Included(bound));
                        }
                    };
                    let (bound_min, bound_max) = if index_meta.is_unique {
                        TableCodec::index_bound(table_name, &index_meta.id)
                    } else {
                        TableCodec::tuple_bound(table_name)
                    };

                    let mut encode_min = bound_encode(min)?;
                    check_bound(&mut encode_min, bound_min);

                    let mut encode_max = bound_encode(max)?;
                    check_bound(&mut encode_max, bound_max);

                    let iter = self.tx.iter(
                        encode_min.as_ref().map(Vec::as_slice),
                        encode_max.as_ref().map(Vec::as_slice),
                    )?;
                    self.scope_iter = Some(iter);
                }
                ConstantBinary::Eq(val) => {
                    let key = self.val_to_key(val)?;
                    if let Some(bytes) = self.tx.get(&key)? {
                        if self.index_meta.is_unique {
                            for tuple_id in TableCodec::decode_index(&bytes)? {
                                self.index_values.push_back(IndexValue::Normal(tuple_id));
                            }
                        } else if self.index_meta.is_primary {
                            let tuple = TableCodec::decode_tuple(self.table.all_columns(), &bytes);

                            self.index_values.push_back(IndexValue::PrimaryKey(tuple));
                        } else {
                            todo!()
                        }
                    }
                    self.scope_iter = None;
                }
                _ => (),
            }
        }
        self.next_tuple()
    }
}

pub trait Iter: Sync + Send {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError>;
}

pub(crate) fn tuple_projection(
    limit: &mut Option<usize>,
    projections: &Projections,
    tuple: Tuple,
) -> Result<Tuple, StorageError> {
    let projection_len = projections.len();
    let mut columns = Vec::with_capacity(projection_len);
    let mut values = Vec::with_capacity(projection_len);

    for expr in projections.iter() {
        values.push(expr.eval(&tuple)?);
        columns.push(expr.output_column());
    }

    if let Some(num) = limit {
        num.sub_assign(1);
    }

    Ok(Tuple {
        id: tuple.id,
        columns,
        values,
    })
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("catalog error")]
    CatalogError(#[from] CatalogError),

    #[error("kipdb error")]
    KipDBError(KernelError),

    #[error("type error")]
    TypeError(#[from] TypeError),

    #[error("The same primary key data already exists")]
    DuplicatePrimaryKey,

    #[error("The column has been declared unique and the value already exists")]
    DuplicateUniqueValue,

    #[error("The table not found")]
    TableNotFound,

    #[error("The some column already exists")]
    DuplicateColumn,

    #[error("Add column must be nullable or specify a default value")]
    NeedNullAbleOrDefault,

    #[error("The table already exists")]
    TableExists,
}

impl From<KernelError> for StorageError {
    fn from(value: KernelError) -> Self {
        StorageError::KipDBError(value)
    }
}

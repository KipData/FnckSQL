pub mod kip;
mod table_codec;

use crate::catalog::{CatalogError, ColumnCatalog, TableCatalog, TableName};
use crate::expression::simplify::ConstantBinary;
use crate::expression::ScalarExpression;
use crate::storage::table_codec::TableCodec;
use crate::types::errors::TypeError;
use crate::types::index::{Index, IndexMetaRef};
use crate::types::tuple::{Tuple, TupleId};
use kip_db::error::CacheError;
use kip_db::kernel::lsm::mvcc;
use kip_db::KernelError;
use std::collections::VecDeque;
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
        table_name: &String,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::IterType<'_>, StorageError>;

    fn read_by_index(
        &self,
        table_name: &String,
        bounds: Bounds,
        projection: Projections,
        index_meta: IndexMetaRef,
        binaries: Vec<ConstantBinary>,
    ) -> Result<IndexIter<'_>, StorageError>;

    fn add_index(
        &mut self,
        table_name: &String,
        index: Index,
        tuple_ids: Vec<TupleId>,
        is_unique: bool,
    ) -> Result<(), StorageError>;

    fn del_index(&mut self, table_name: &String, index: &Index) -> Result<(), StorageError>;

    fn append(
        &mut self,
        table_name: &String,
        tuple: Tuple,
        is_overwrite: bool,
    ) -> Result<(), StorageError>;

    fn delete(&mut self, table_name: &String, tuple_id: TupleId) -> Result<(), StorageError>;

    fn create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
    ) -> Result<TableName, StorageError>;

    fn drop_table(&mut self, table_name: &String) -> Result<(), StorageError>;
    fn drop_data(&mut self, table_name: &String) -> Result<(), StorageError>;
    fn table(&self, table_name: &String) -> Option<&TableCatalog>;

    fn show_tables(&self) -> Result<Vec<String>, StorageError>;

    #[allow(async_fn_in_trait)]
    async fn commit(self) -> Result<(), StorageError>;
}

// TODO: Table return optimization
pub struct IndexIter<'a> {
    projections: Projections,
    table: &'a TableCatalog,
    tuple_ids: VecDeque<TupleId>,
    tx: &'a mvcc::Transaction,
}

impl Iter for IndexIter<'_> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError> {
        if let Some(tuple_id) = self.tuple_ids.pop_front() {
            let key = TableCodec::encode_tuple_key(&self.table.name, &tuple_id)?;

            Ok(self
                .tx
                .get(&key)?
                .map(|bytes| {
                    let tuple = TableCodec::decode_tuple(self.table.all_columns(), &bytes);

                    tuple_projection(&mut None, &self.projections, tuple)
                })
                .transpose()?)
        } else {
            Ok(None)
        }
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
        values.push(expr.eval_column(&tuple)?);
        columns.push(expr.output_columns());
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

    #[error("cache error")]
    CacheError(CacheError),

    #[error("type error")]
    TypeError(#[from] TypeError),

    #[error("The same primary key data already exists")]
    DuplicatePrimaryKey,

    #[error("The column has been declared unique and the value already exists")]
    DuplicateUniqueValue,

    #[error("The table not found")]
    TableNotFound,
}

impl From<KernelError> for StorageError {
    fn from(value: KernelError) -> Self {
        StorageError::KipDBError(value)
    }
}

impl From<CacheError> for StorageError {
    fn from(value: CacheError) -> Self {
        StorageError::CacheError(value)
    }
}

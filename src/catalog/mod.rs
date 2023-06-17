use std::collections::HashMap;
// Module: catalog
pub(crate) use self::column::*;
pub(crate) use self::database::*;
pub(crate) use self::root::*;
pub(crate) use self::schema::*;
pub(crate) use self::table::*;

use crate::types::{CatalogId, ColumnId};
use std::sync::Arc;
use parking_lot::Mutex;

/// The type of catalog reference.
pub(crate) type CatalogRef = Arc<RootCatalog>;

pub(crate) static DEFAULT_DATABASE_NAME: &str = "kip_sql";
pub(crate) static DEFAULT_SCHEMA_NAME: &str = "kip_sql";

mod column;
mod database;
mod root;
mod schema;
mod table;

/// The reference ID of a table.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct TableRefId {
    pub schema_id: CatalogId,
    pub table_id: CatalogId,
}

impl TableRefId {
    pub const fn new(schema_id: CatalogId, table_id: CatalogId) -> Self {
        TableRefId {
            schema_id,
            table_id,
        }
    }
}

/// The reference ID of a column.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct ColumnRefId {
    pub schema_id: CatalogId,
    pub table_id: CatalogId,
    pub column_id: ColumnId,
}

impl ColumnRefId {
    pub const fn from_table(table: TableRefId, column_id: ColumnId) -> Self {
        ColumnRefId {
            schema_id: table.schema_id,
            table_id: table.table_id,
            column_id,
        }
    }

    pub const fn new(schema_id: CatalogId, table_id: CatalogId, column_id: ColumnId) -> Self {
        ColumnRefId {
            schema_id,
            table_id,
            column_id,
        }
    }
}

/// The error type of catalog operations.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}

pub(crate) trait Catalog<T>: Sized {
    fn add(&self, name: String, item: T) -> Result<i64, CatalogError>;

    fn delete(&self, name: &str) -> Result<(), CatalogError>;

    fn all(&self) -> Vec<Arc<T>>;

    fn get_id_by_name(&self, name: &str) -> Option<i64>;

    fn get_by_id(&self, id: i64) -> Option<Arc<T>>;

    fn get_by_name(&self, name: &str) -> Option<Arc<T>>;

    fn name(&self) -> &str;

    fn id(&self) -> i64;

    fn set_id(&mut self, id: i64);
}

pub(crate) struct CatalogTemp<T> {
    id: i64,
    name: String,
    err_topic: &'static str,
    inner: Mutex<Inner<T>>,
}

struct Inner<T> {
    idxs: HashMap<String, i64>,
    item_idxs: HashMap<i64, Arc<T>>,
    next_id: i64,
}

impl<T> CatalogTemp<T> {
    fn new(name: String, err_msg: &'static str) -> Self {
        Self {
            id: 0,
            name,
            err_topic: err_msg,
            inner: Mutex::new(Inner {
                idxs: Default::default(),
                item_idxs: Default::default(),
                next_id: 0,
            }),
        }
    }
}

impl<T> Catalog<T> for CatalogTemp<T> {
    fn add(&self, name: String, item: T) -> Result<i64, CatalogError> {
        let mut inner = self.inner.lock();
        if inner.idxs.contains_key(&name) {
            return Err(CatalogError::Duplicated(self.err_topic, name));
        }
        let id = inner.next_id;

        inner.next_id += 1;
        inner.idxs.insert(name.clone(), id);
        inner.item_idxs.insert(id, Arc::new(item));

        Ok(id)
    }

    fn delete(&self, name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.lock();

        let id = inner
            .idxs
            .remove(name)
            .ok_or_else(|| CatalogError::NotFound(self.err_topic, name.into()))?;
        inner.item_idxs.remove(&id);

        Ok(())
    }

    fn all(&self) -> Vec<Arc<T>> {
        self.inner.lock()
            .item_idxs
            .values()
            .map(Arc::clone)
            .collect()
    }

    fn get_id_by_name(&self, name: &str) -> Option<i64> {
        self.inner.lock()
            .idxs.get(name)
            .cloned()
    }

    fn get_by_id(&self, id: i64) -> Option<Arc<T>> {
        self.inner.lock()
            .item_idxs
            .get(&id)
            .cloned()
    }

    fn get_by_name(&self, name: &str) -> Option<Arc<T>> {
        let inner = self.inner.lock();

        inner.idxs
            .get(name)
            .and_then(|id| inner.item_idxs.get(id))
            .cloned()
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn id(&self) -> i64 {
        self.id
    }

    fn set_id(&mut self, id: i64) {
        self.id = id;
    }
}

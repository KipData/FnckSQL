use crate::catalog::{CatalogRef, Column, Root, TableRefId};
use crate::types::ColumnId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum StorageError {
    #[error("failed to read table")]
    ReadTableError,
    #[error("failed to write table")]
    WriteTableError,
    #[error("{0}({1}) not found")]
    NotFound(&'static str, u32),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
    #[error("invalid column id: {0}")]
    InvalidColumn(ColumnId),
}

pub trait Storage: Sync + Send {
    fn create_table(&mut self, table_name: &str, columns: &Vec<Column>)
        -> Result<(), StorageError>;
}

pub type StorageRef = Arc<dyn Storage>;

#[derive(Debug, Clone)]
pub struct InMemoryStorage {
    pub catalog: Root,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            catalog: Root::new(),
        }
    }

    pub fn catalog(&self) -> &Root {
        &self.catalog
    }
}

impl Storage for InMemoryStorage {
    fn create_table(
        &mut self,
        table_name: &str,
        column_descs: &Vec<Column>,
    ) -> Result<(), StorageError> {
        let table_id = self
            .catalog
            .add_table(table_name.into(), column_descs.to_vec())
            .map_err(|_| StorageError::Duplicated("table", table_name.into()))?;
        Ok(())
    }
}

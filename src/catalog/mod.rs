use crate::catalog::column_catalog::ColumnCatalog;
use std::sync::Arc;

mod column_catalog;
mod schema_catalog;
mod table_catalog;
pub(crate) type ColumnCatalogRef = Arc<ColumnCatalog>;

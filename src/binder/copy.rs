use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use crate::errors::DatabaseError;
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::operator::Operator;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{CopyOption, CopySource, CopyTarget};

use super::*;

#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, Serialize, Deserialize)]
pub struct ExtSource {
    pub path: PathBuf,
    pub format: FileFormat,
}

/// File format.
#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, Serialize, Deserialize)]
pub enum FileFormat {
    Csv {
        /// Delimiter to parse.
        delimiter: char,
        /// Quote to use.
        quote: char,
        /// Escape character to use.
        escape: Option<char>,
        /// Whether or not the file has a header line.
        header: bool,
    },
}

impl std::fmt::Display for ExtSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for ExtSource {
    type Err = ();
    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Err(())
    }
}

impl<'a, 'b, T: Transaction> Binder<'a, 'b, T> {
    pub(super) fn bind_copy(
        &mut self,
        source: CopySource,
        to: bool,
        target: CopyTarget,
        options: &[CopyOption],
    ) -> Result<LogicalPlan, DatabaseError> {
        let (table_name, ..) = match source {
            CopySource::Table {
                table_name,
                columns,
            } => (table_name, columns),
            CopySource::Query(_) => {
                return Err(DatabaseError::UnsupportedStmt("'COPY SOURCE'".to_string()));
            }
        };

        if let Some(table) = self.context.table(Arc::new(table_name.to_string())) {
            let schema_ref = table.schema_ref().clone();
            let ext_source = ExtSource {
                path: match target {
                    CopyTarget::File { filename } => filename.into(),
                    t => todo!("unsupported copy target: {:?}", t),
                },
                format: FileFormat::from_options(options),
            };

            if to {
                // COPY <source_table> TO <dest_file>
                Ok(LogicalPlan::new(
                    Operator::CopyToFile(CopyToFileOperator { source: ext_source }),
                    vec![],
                ))
            } else {
                // COPY <dest_table> FROM <source_file>
                Ok(LogicalPlan::new(
                    Operator::CopyFromFile(CopyFromFileOperator {
                        source: ext_source,
                        schema_ref,
                        table: table_name.to_string(),
                    }),
                    vec![],
                ))
            }
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }
}

impl FileFormat {
    /// Create from copy options.
    pub fn from_options(options: &[CopyOption]) -> Self {
        let mut delimiter = ',';
        let mut quote = '"';
        let mut escape = None;
        let mut header = false;
        for opt in options {
            match opt {
                CopyOption::Format(fmt) => {
                    debug_assert_eq!(fmt.value.to_lowercase(), "csv", "only support CSV format")
                }
                CopyOption::Delimiter(c) => delimiter = *c,
                CopyOption::Header(b) => header = *b,
                CopyOption::Quote(c) => quote = *c,
                CopyOption::Escape(c) => escape = Some(*c),
                o => panic!("unsupported copy option: {:?}", o),
            }
        }
        FileFormat::Csv {
            delimiter,
            quote,
            escape,
            header,
        }
    }
}

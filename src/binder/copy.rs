use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sqlparser::ast::{CopyOption, CopySource, CopyTarget};
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::operator::Operator;

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
    fn from_str(_s: &str) -> std::result::Result<Self, Self::Err> {
        Err(())
    }
}

impl<S: Storage> Binder<S> {
    pub(super) async fn bind_copy(
        &mut self,
        source: CopySource,
        to: bool,
        target: CopyTarget,
        options: &[CopyOption],
    ) -> Result<LogicalPlan, BindError> {
        let (table_name,..) = match source {
            CopySource::Table {
                table_name,
                columns,
            } => (table_name, columns),
            CopySource::Query(_) => {
                return Err(BindError::UnsupportedCopySource(
                    "bad copy source".to_string(),
                ));
            }
        };

        if let Some(table) = self.context.storage.table(&table_name.to_string()).await {
            let cols = table.all_columns();
            let ext_source = ExtSource {
                path: match target {
                    CopyTarget::File { filename } => filename.into(),
                    t => todo!("unsupported copy target: {:?}", t),
                },
                format: FileFormat::from_options(options),
            };
            let types = cols.iter().map(|c| c.desc.column_datatype).collect();

            let copy = if to {
                // COPY <source_table> TO <dest_file>
                LogicalPlan {
                    operator: Operator::CopyToFile(
                        CopyToFileOperator {
                            source: ext_source,
                        }
                    ),
                    childrens: vec![],
                }
            } else {
                // COPY <dest_table> FROM <source_file>
                LogicalPlan {
                    operator: Operator::CopyFromFile(
                        CopyFromFileOperator {
                            source: ext_source,
                            types,
                            columns: cols,
                            table: table_name.to_string()
                        }
                    ),
                    childrens: vec![],
                }
            };
            Ok(copy)
        } else {
            Err(BindError::InvalidTable(format!("not found table {}", table_name)))
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
                    assert_eq!(fmt.value.to_lowercase(), "csv", "only support CSV format")
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

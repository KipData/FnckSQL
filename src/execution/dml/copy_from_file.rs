use crate::binder::copy::FileFormat;
use crate::errors::DatabaseError;
use crate::execution::{Executor, WriteExecutor};
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction};
use crate::throw;
use crate::types::tuple::{types, Tuple};
use crate::types::tuple_builder::TupleBuilder;
use std::fs::File;
use std::io::BufReader;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;

pub struct CopyFromFile {
    op: CopyFromFileOperator,
    size: usize,
}

impl From<CopyFromFileOperator> for CopyFromFile {
    fn from(op: CopyFromFileOperator) -> Self {
        CopyFromFile { op, size: 0 }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CopyFromFile {
    fn execute_mut(
        self,
        _: (&'a TableCache, &'a StatisticsMetaCache),
        transaction: &'a mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let types = types(&self.op.schema_ref);
                let (tx, rx) = mpsc::channel();
                let (tx1, rx1) = mpsc::channel();
                // # Cancellation
                // When this stream is dropped, the `rx` is dropped, the spawned task will fail to send to
                // `tx`, then the task will finish.
                let table_name = self.op.table.clone();
                let handle = thread::spawn(|| self.read_file_blocking(tx));
                let mut size = 0_usize;
                while let Ok(chunk) = rx.recv() {
                    throw!(transaction.append(&table_name, chunk, &types, false));
                    size += 1;
                }
                throw!(handle.join().unwrap());

                let handle = thread::spawn(move || return_result(size, tx1));
                while let Ok(chunk) = rx1.recv() {
                    yield Ok(chunk);
                }
                throw!(handle.join().unwrap())
            },
        )
    }
}

impl CopyFromFile {
    /// Read records from file using blocking IO.
    ///
    /// The read data chunks will be sent through `tx`.
    fn read_file_blocking(mut self, tx: Sender<Tuple>) -> Result<(), DatabaseError> {
        let file = File::open(self.op.source.path)?;
        let mut buf_reader = BufReader::new(file);
        let mut reader = match self.op.source.format {
            FileFormat::Csv {
                delimiter,
                quote,
                escape,
                header,
            } => csv::ReaderBuilder::new()
                .delimiter(delimiter as u8)
                .quote(quote as u8)
                .escape(escape.map(|c| c as u8))
                .has_headers(header)
                .from_reader(&mut buf_reader),
        };

        let column_count = self.op.schema_ref.len();
        let tuple_builder = TupleBuilder::new(&self.op.schema_ref);

        for record in reader.records() {
            // read records and push raw str rows into data chunk builder
            let record = record?;

            if !(record.len() == column_count
                || record.len() == column_count + 1 && record.get(column_count) == Some(""))
            {
                return Err(DatabaseError::MisMatch("columns", "values"));
            }

            self.size += 1;
            tx.send(tuple_builder.build_with_row(record.iter())?)
                .map_err(|_| DatabaseError::ChannelClose)?;
        }
        Ok(())
    }
}

fn return_result(size: usize, tx: Sender<Tuple>) -> Result<(), DatabaseError> {
    let tuple = TupleBuilder::build_result(format!("import {} rows", size));

    tx.send(tuple).map_err(|_| DatabaseError::ChannelClose)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnSummary};
    use crate::db::DataBaseBuilder;
    use sqlparser::ast::CharLengthUnits;
    use std::io::Write;
    use std::ops::{Coroutine, CoroutineState};
    use std::pin::Pin;
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::*;
    use crate::binder::copy::ExtSource;
    use crate::errors::DatabaseError;
    use crate::storage::Storage;
    use crate::types::LogicalType;

    #[test]
    fn read_csv() -> Result<(), DatabaseError> {
        let csv = "1,1.5,one\n2,2.5,two\n";

        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp file");
        write!(file, "{}", csv).expect("failed to write file");

        let columns = vec![
            Arc::new(ColumnCatalog {
                summary: ColumnSummary {
                    id: Some(0),
                    name: "a".to_string(),
                    table_name: None,
                },
                nullable: false,
                desc: ColumnDesc::new(LogicalType::Integer, true, false, None),
            }),
            Arc::new(ColumnCatalog {
                summary: ColumnSummary {
                    id: Some(1),
                    name: "b".to_string(),
                    table_name: None,
                },
                nullable: false,
                desc: ColumnDesc::new(LogicalType::Float, false, false, None),
            }),
            Arc::new(ColumnCatalog {
                summary: ColumnSummary {
                    id: Some(1),
                    name: "c".to_string(),
                    table_name: None,
                },
                nullable: false,
                desc: ColumnDesc::new(
                    LogicalType::Varchar(Some(10), CharLengthUnits::Characters),
                    false,
                    false,
                    None,
                ),
            }),
        ];

        let op = CopyFromFileOperator {
            table: "test_copy".to_string(),
            source: ExtSource {
                path: file.path().into(),
                format: FileFormat::Csv {
                    delimiter: ',',
                    quote: '"',
                    escape: None,
                    header: false,
                },
            },
            schema_ref: Arc::new(columns),
        };
        let executor = CopyFromFile {
            op: op.clone(),
            size: 0,
        };

        let temp_dir = TempDir::new().unwrap();
        let db = DataBaseBuilder::path(temp_dir.path()).build()?;
        let _ = db.run("create table test_copy (a int primary key, b float, c varchar(10))");
        let storage = db.storage;
        let mut transaction = storage.transaction()?;

        let mut coroutine =
            executor.execute_mut((&db.table_cache, &db.meta_cache), &mut transaction);
        let tuple = match Pin::new(&mut coroutine).resume(()) {
            CoroutineState::Yielded(tuple) => tuple,
            CoroutineState::Complete(()) => unreachable!(),
        }
        .unwrap();
        debug_assert_eq!(
            tuple,
            TupleBuilder::build_result(format!("import {} rows", 2))
        );

        Ok(())
    }
}

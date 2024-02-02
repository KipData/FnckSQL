use crate::binder::copy::FileFormat;
use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, WriteExecutor};
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use futures_async_stream::try_stream;
use std::fs::File;
use std::io::BufReader;
use tokio::sync::mpsc::Sender;

pub struct CopyFromFile {
    op: CopyFromFileOperator,
    size: usize,
}

impl From<CopyFromFileOperator> for CopyFromFile {
    fn from(op: CopyFromFileOperator) -> Self {
        CopyFromFile { op, size: 0 }
    }
}

impl<T: Transaction> WriteExecutor<T> for CopyFromFile {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl CopyFromFile {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let (tx1, mut rx1) = tokio::sync::mpsc::channel(1);
        // # Cancellation
        // When this stream is dropped, the `rx` is dropped, the spawned task will fail to send to
        // `tx`, then the task will finish.
        let table_name = self.op.table.clone();
        let handle = tokio::task::spawn_blocking(|| self.read_file_blocking(tx));
        let mut size = 0_usize;
        while let Some(chunk) = rx.recv().await {
            transaction.append(&table_name, chunk, false)?;
            size += 1;
        }
        handle.await??;

        let handle = tokio::task::spawn_blocking(move || return_result(size, tx1));
        while let Some(chunk) = rx1.recv().await {
            yield chunk;
        }
        handle.await??;
    }
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

        let column_count = self.op.columns.len();
        let tuple_builder = TupleBuilder::new(self.op.columns.clone());

        for record in reader.records() {
            // read records and push raw str rows into data chunk builder
            let record = record?;

            if !(record.len() == column_count
                || record.len() == column_count + 1 && record.get(column_count) == Some(""))
            {
                return Err(DatabaseError::LengthMismatch {
                    expected: column_count,
                    actual: record.len(),
                });
            }

            self.size += 1;
            tx.blocking_send(tuple_builder.build_with_row(record.iter())?)
                .map_err(|_| DatabaseError::ChannelClose)?;
        }
        Ok(())
    }
}

fn return_result(size: usize, tx: Sender<Tuple>) -> Result<(), DatabaseError> {
    let tuple = TupleBuilder::build_result(
        "COPY FROM SOURCE".to_string(),
        format!("import {} rows", size),
    )?;

    tx.blocking_send(tuple)
        .map_err(|_| DatabaseError::ChannelClose)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnSummary};
    use crate::db::Database;
    use futures::StreamExt;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::*;
    use crate::binder::copy::ExtSource;
    use crate::errors::DatabaseError;
    use crate::storage::Storage;
    use crate::types::LogicalType;

    #[tokio::test]
    async fn read_csv() -> Result<(), DatabaseError> {
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
                ref_expr: None,
            }),
            Arc::new(ColumnCatalog {
                summary: ColumnSummary {
                    id: Some(1),
                    name: "b".to_string(),
                    table_name: None,
                },
                nullable: false,
                desc: ColumnDesc::new(LogicalType::Float, false, false, None),
                ref_expr: None,
            }),
            Arc::new(ColumnCatalog {
                summary: ColumnSummary {
                    id: Some(1),
                    name: "c".to_string(),
                    table_name: None,
                },
                nullable: false,
                desc: ColumnDesc::new(LogicalType::Varchar(Some(10)), false, false, None),
                ref_expr: None,
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

            columns,
        };
        let executor = CopyFromFile {
            op: op.clone(),
            size: 0,
        };

        let temp_dir = TempDir::new().unwrap();
        let db = Database::with_kipdb(temp_dir.path()).await?;
        let _ = db
            .run("create table test_copy (a int primary key, b float, c varchar(10))")
            .await;
        let storage = db.storage;
        let mut transaction = storage.transaction().await?;

        let tuple = executor
            .execute_mut(&mut transaction)
            .next()
            .await
            .unwrap()?;
        assert_eq!(
            tuple,
            TupleBuilder::build_result(
                "COPY FROM SOURCE".to_string(),
                format!("import {} rows", 2)
            )
            .unwrap()
        );

        Ok(())
    }
}

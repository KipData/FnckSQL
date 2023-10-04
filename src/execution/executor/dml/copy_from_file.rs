use crate::binder::copy::FileFormat;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::storage::{Storage, Transaction};
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

impl<S: Storage> Executor<S> for CopyFromFile {
    fn execute(self, storage: &S) -> BoxedExecutor {
        self._execute(storage.clone())
    }
}

impl CopyFromFile {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<S: Storage>(self, storage: S) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let (tx1, mut rx1) = tokio::sync::mpsc::channel(1);
        // # Cancellation
        // When this stream is dropped, the `rx` is dropped, the spawned task will fail to send to
        // `tx`, then the task will finish.
        let table_name = self.op.table.clone();
        if let Some(mut txn) = storage.transaction(&table_name).await {
            let handle = tokio::task::spawn_blocking(|| self.read_file_blocking(tx));
            let mut size = 0 as usize;
            while let Some(chunk) = rx.recv().await {
                txn.append(chunk, false)?;
                size += 1;
            }
            handle.await?;
            txn.commit().await?;

            let handle = tokio::task::spawn_blocking(move || return_result(size.clone(), tx1));
            while let Some(chunk) = rx1.recv().await {
                yield chunk;
            }
            handle.await?;
        }
    }
    /// Read records from file using blocking IO.
    ///
    /// The read data chunks will be sent through `tx`.
    fn read_file_blocking(mut self, tx: Sender<Tuple>) -> Result<(), ExecutorError> {
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

        let column_count = self.op.types.len();
        let mut size_count = 0;

        for record in reader.records() {
            let mut tuple_builder =
                TupleBuilder::new(self.op.types.clone(), self.op.columns.clone());
            // read records and push raw str rows into data chunk builder
            let record = record?;

            if !(record.len() == column_count
                || record.len() == column_count + 1 && record.get(column_count) == Some(""))
            {
                return Err(ExecutorError::LengthMismatch {
                    expected: column_count,
                    actual: record.len(),
                });
            }

            size_count += 1;

            // push a raw str row and send it if necessary
            if let Some(chunk) = tuple_builder.push_str_row(record.iter())? {
                tx.blocking_send(chunk).map_err(|_| ExecutorError::Abort)?;
            }
        }
        self.size = size_count;
        Ok(())
    }
}

fn return_result(size: usize, tx: Sender<Tuple>) -> Result<(), ExecutorError> {
    let tuple_builder = TupleBuilder::new_result();
    let tuple =
        tuple_builder.push_result("COPY FROM SOURCE", format!("import {} rows", size).as_str())?;
    tx.blocking_send(tuple).map_err(|_| ExecutorError::Abort)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::Database;
    use futures::StreamExt;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::*;
    use crate::binder::copy::ExtSource;
    use crate::types::LogicalType;

    #[tokio::test]
    async fn read_csv() {
        let csv = "1,1.5,one\n2,2.5,two\n";

        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp file");
        write!(file, "{}", csv).expect("failed to write file");

        let columns = vec![
            Arc::new(ColumnCatalog {
                id: Some(0),
                name: "a".to_string(),
                table_name: None,
                nullable: false,
                desc: ColumnDesc::new(LogicalType::Integer, true, false),
                ref_expr: None,
            }),
            Arc::new(ColumnCatalog {
                id: Some(1),
                name: "b".to_string(),
                table_name: None,
                nullable: false,
                desc: ColumnDesc::new(LogicalType::Float, false, false),
                ref_expr: None,
            }),
            Arc::new(ColumnCatalog {
                id: Some(1),
                name: "c".to_string(),
                table_name: None,
                nullable: false,
                desc: ColumnDesc::new(LogicalType::Varchar(Some(10)), false, false),
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

            types: vec![
                LogicalType::Integer,
                LogicalType::Float,
                LogicalType::Varchar(Some(10)),
            ],
            columns: columns.clone(),
        };
        let executor = CopyFromFile {
            op: op.clone(),
            size: 0,
        };

        let temp_dir = TempDir::new().unwrap();
        let db = Database::with_kipdb(temp_dir.path()).await.unwrap();
        let _ = db
            .run("create table test_copy (a int primary key, b float, c varchar(10))")
            .await;
        let actual = executor.execute(&db.storage).next().await.unwrap().unwrap();

        let tuple_builder = TupleBuilder::new_result();
        let expected = tuple_builder
            .push_result("COPY FROM SOURCE", format!("import {} rows", 2).as_str())
            .unwrap();
        assert_eq!(actual, expected);
    }
}

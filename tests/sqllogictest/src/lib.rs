use fnck_sql::db::Database;
use fnck_sql::errors::DatabaseError;
use fnck_sql::storage::rocksdb::RocksStorage;
use sqllogictest::{DBOutput, DefaultColumnType, DB};
use std::time::Instant;

pub struct SQLBase {
    pub db: Database<RocksStorage>,
}

impl DB for SQLBase {
    type Error = DatabaseError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let start = Instant::now();
        let (schema, tuples) = self.db.run(sql)?;
        println!("|— Input SQL: {}", sql);
        println!(" |— time spent: {:?}", start.elapsed());

        if tuples.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }

        let types = vec![DefaultColumnType::Any; schema.len()];
        let rows = tuples
            .into_iter()
            .map(|tuple| {
                tuple
                    .values
                    .into_iter()
                    .map(|value| format!("{}", value))
                    .collect()
            })
            .collect();
        Ok(DBOutput::Rows { types, rows })
    }
}

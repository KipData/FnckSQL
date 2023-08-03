#![feature(iterator_try_collect)]
use std::sync::Arc;

use sqllogictest::{AsyncDB, Runner};
use kip_sql::db::{Database,DatabaseError};
use kip_sql::util::record_batch_to_string;
pub fn test_run(sqlfile: &str){
    
    let db = Arc::new( Database::new_on_mem());
    let mut tester = Runner::new(DatabaseWrapper {db});
    tester.run_file(sqlfile).unwrap()
}

struct DatabaseWrapper {
    db: Arc<Database>,
}

#[async_trait::async_trait]
impl AsyncDB for DatabaseWrapper {
    type Error = DatabaseError;
    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        let chunks = self.db.run(sql).await.unwrap();
        let output = chunks.iter().map(record_batch_to_string).try_collect();
        Ok(output?)
    }
    
}
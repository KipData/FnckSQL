
use std::sync::Arc;

use sqllogictest::{AsyncDB, Runner};
use kip_sql::db::{Database,DatabaseError};
use kip_sql::util::record_batch_to_string;
fn init_tables(){
    
    let db = Arc::new( Database::new_on_mem());
    let mut tester = Runner::new(DatabaseWrapper {db});
    tester.run_file(sqlfile).unwrap()
    // tokio_test::block_on(async move {
    //     let _ = database.run("create table t1 (a int, b boolean)").await?;
    //     let _ = database.run("insert into t1 values (1, true), (2, false)").await?;
    //     let vec_batch = database.run("select * from t1").await?;

    //     let table = database.storage
    //         .get_catalog()
    //         .get_table(0).unwrap().clone();
    //     println!("{:#?}", concat_batches(&table.schema(), &vec_batch));

    //     Ok(())
    // })

}

struct DatabaseWrapper {
    db: Arc<Database>,
}

#[async_trait::async_trait]
impl AsyncDB for DatabaseWrapper {
    type Error = DatabaseError;
    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        let chunks = self.db.run(sql).await?;
        let output = chunks.iter().map(record_batch_to_string).try_collect();
        Ok(output?)
    }
}
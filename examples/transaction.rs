use fnck_sql::db::Database;
use fnck_sql::errors::DatabaseError;

#[tokio::main]
async fn main() -> Result<(), DatabaseError> {
    let database = Database::with_kipdb("./transaction").await?;
    let mut tx_1 = database.new_transaction().await?;

    let _ = tx_1
        .run("create table if not exists t1 (c1 int primary key, c2 int)")
        .await?;
    let _ = tx_1.run("insert into t1 values(0, 0), (1, 1)").await?;

    assert!(database.run("select * from t1").await.is_err());

    tx_1.commit().await?;

    assert!(database.run("select * from t1").await.is_ok());

    let _ = database.run("drop table t1").await?;

    Ok(())
}

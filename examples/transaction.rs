use fnck_sql::db::DataBaseBuilder;
use fnck_sql::errors::DatabaseError;

fn main() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path("./transaction").build()?;
    let mut tx_1 = database.new_transaction()?;

    let _ = tx_1.run("create table if not exists t1 (c1 int primary key, c2 int)")?;
    let _ = tx_1.run("insert into t1 values(0, 0), (1, 1)")?;

    assert!(database.run("select * from t1").is_err());

    tx_1.commit()?;

    assert!(database.run("select * from t1").is_ok());

    let _ = database.run("drop table t1")?;

    Ok(())
}

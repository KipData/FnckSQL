use fnck_sql::db::DataBaseBuilder;
use fnck_sql::errors::DatabaseError;
use fnck_sql::implement_from_tuple;
use fnck_sql::types::tuple::{SchemaRef, Tuple};
use fnck_sql::types::value::DataValue;
use fnck_sql::types::LogicalType;
use itertools::Itertools;

#[derive(Default, Debug, PartialEq)]
struct MyStruct {
    pub c1: i32,
    pub c2: String,
}

implement_from_tuple!(
    MyStruct, (
        c1: i32 => |inner: &mut MyStruct, value| {
            if let DataValue::Int32(Some(val)) = value {
                inner.c1 = val;
            }
        },
        c2: String => |inner: &mut MyStruct, value| {
            if let DataValue::Utf8(Some(val)) = value {
                inner.c2 = val;
            }
        }
    )
);

#[cfg(feature = "marcos")]
#[tokio::main]
async fn main() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path("./hello_world").build().await?;

    let _ = database
        .run("create table if not exists my_struct (c1 int primary key, c2 int)")
        .await?;
    let _ = database
        .run("insert into my_struct values(0, 0), (1, 1)")
        .await?;
    let (schema, tuples) = database.run("select * from my_struct").await?;
    let tuples = tuples
        .into_iter()
        .map(|tuple| MyStruct::from((&schema, tuple)))
        .collect_vec();

    println!("{:#?}", tuples);

    let _ = database.run("drop table my_struct").await?;

    Ok(())
}

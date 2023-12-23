use itertools::Itertools;
use kip_sql::db::{Database, DatabaseError};
use kip_sql::implement_from_tuple;
use kip_sql::types::tuple::Tuple;
use kip_sql::types::value::DataValue;
use kip_sql::types::LogicalType;

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

#[tokio::main]
async fn main() -> Result<(), DatabaseError> {
    let database = Database::with_kipdb("./hello_world").await?;

    let _ = database
        .run("create table if not exists my_struct (c1 int primary key, c2 int)")
        .await?;
    let _ = database
        .run("insert into my_struct values(0, 0), (1, 1)")
        .await?;
    let tuples = database
        .run("select * from my_struct")
        .await?
        .into_iter()
        .map(MyStruct::from)
        .collect_vec();

    println!("{:#?}", tuples);

    let _ = database.run("drop table my_struct").await?;

    Ok(())
}

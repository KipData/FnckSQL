<pre align="center">
Built by @KipData


███████╗███╗   ██╗ ██████╗██╗  ██╗    ███████╗ ██████╗ ██╗     
██╔════╝████╗  ██║██╔════╝██║ ██╔╝    ██╔════╝██╔═══██╗██║     
█████╗  ██╔██╗ ██║██║     █████╔╝     ███████╗██║   ██║██║     
██╔══╝  ██║╚██╗██║██║     ██╔═██╗     ╚════██║██║▄▄ ██║██║     
██║     ██║ ╚████║╚██████╗██║  ██╗    ███████║╚██████╔╝███████╗
╚═╝     ╚═╝  ╚═══╝ ╚═════╝╚═╝  ╚═╝    ╚══════╝ ╚══▀▀═╝ ╚══════╝

-----------------------------------
🖕
</pre>
<h3 align="center">
    Lightweight DBMS
</h3>

<p align="center">
    &nbsp;
    <a href="https://github.com/KipData/KipSQL/actions/workflows/ci.yml"><img src="https://github.com/KipData/KipSQL/actions/workflows/ci.yml/badge.svg" alt="CI"></img></a>
    &nbsp;
    <a href="https://github.com/KipData/KipSQL/blob/main/LICENSE"><img src="https://img.shields.io/github/license/KipData/KipSQL"></a>
    &nbsp;
    <a href="https://www.rust-lang.org/community"><img src="https://img.shields.io/badge/Rust_Community%20-Join_us-brightgreen?style=plastic&logo=rust"></a>
    <a href="https://crates.io/crates/fnck_sql/"><img src="https://img.shields.io/crates/v/fnck_sql.svg"></a>
</p>
<p align="center">
  <a href="https://github.com/KipData/KipSQL" target="_blank">
    <img src="https://img.shields.io/github/stars/KipData/KipSQL.svg?style=social" alt="github star"/>
    <img src="https://img.shields.io/github/forks/KipData/KipSQL.svg?style=social" alt="github fork"/>
  </a>
</p>

### What is FnckSQL

FnckSQL individual developers independently implemented LSM KV-based SQL DBMS out of hobby. This SQL database will prove to you that anyone can write a database (even the core author cannot find a job). If you are also a database-related Enthusiastic, let us give this "beautiful" industry a middle finger🖕.

Welcome to our WebSite, Power By FnckSQL: **http://www.kipdata.site/**

### Quick Started
Tips: Install rust toolchain first.

Clone the repository
``` shell
git clone https://github.com/KipData/FnckSQL.git
```
![start](./static/images/start.gif)
then use `psql` to enter sql
![pg](./static/images/pg.gif)
Using FnckSQL in code
```rust
let fnck_sql = Database::with_kipdb("./data").await?;
let tupes = fnck_sql.run("select * from t1").await?;
```
Storage Support:
- KipDB

### Features
- ORM Mapping: `features = ["marcos"]`
```rust
#[derive(Default, Debug, PartialEq)]
struct MyStruct {
  c1: i32,
  c2: String,
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
```
- Optimizer
  - RBO
  - CBO based on RBO(Physical Selection)
- Execute
  - Volcano
  - Codegen on LuaJIT: `features = ["codegen_execute"]`
- MVCC Transaction
  - Optimistic
- Field options
  - [not] null
  - unique
  - primary key
- SQL where options
  - is [not] null
  - [not] like
  - [not] in
- Supports index type
  - Unique Index
- Supports multiple primary key types
  - Tinyint
  - UTinyint
  - Smallint
  - USmallint
  - Integer
  - UInteger
  - Bigint
  - UBigint
  - Varchar
- DDL
  - Begin (Server only)
  - Commit (Server only)
  - Rollback (Server only)
  - Create
    - [x] Table
    - [ ] Index
  - Drop
    - [x] Table
    - [ ] Index
  - Alert
    - [x] Add Column
    - [x] Drop Column
  - [x] Truncate
- DQL
  - [x] Select
    - SeqScan
    - IndexScan
  - [x] Where
  - [x] Distinct
  - [x] Alias
  - [x] Aggregation: count()/sum()/avg()/min()/max()
  - [x] SubQuery(from)
  - [x] Join: Inner/Left/Right/Full Cross(x)
  - [x] Group By
  - [x] Having
  - [x] Order By
  - [x] Limit
  - [x] Show Tables
  - [x] Explain
- DML
  - [x] Insert
  - [x] Insert Overwrite
  - [x] Update
  - [x] Delete
  - [x] Analyze
- DataTypes
  - Invalid
  - SqlNull
  - Boolean
  - Tinyint
  - UTinyint
  - Smallint
  - USmallint
  - Integer
  - UInteger
  - Bigint
  - UBigint
  - Float
  - Double
  - Varchar
  - Date
  - DateTime

## License

FnckSQL uses the [Apache 2.0 license][1] to strike a balance between
open contributions and allowing you to use the software however you want.

[1]: <https://github.com/KipData/KipSQL/blob/main/LICENSE>

### Thanks For
- [Fedomn/sqlrs](https://github.com/Fedomn/sqlrs): Main reference materials, Optimizer and Executor all refer to the design of sqlrs
- [systemxlabs/bustubx](https://github.com/systemxlabs/bustubx)
- [duckdb/duckdb](https://github.com/duckdb/duckdb)

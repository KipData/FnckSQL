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
    <a href="https://summer-ospp.ac.cn/org/orgdetail/0b09d23d-2510-4537-aa9d-45158bb6bdc2"><img src="https://img.shields.io/badge/OSPP-KipData-3DA639?logo=opensourceinitiative"></a>
    <a href="https://github.com/KipData/KipSQL/blob/main/LICENSE"><img src="https://img.shields.io/github/license/KipData/KipSQL"></a>
    &nbsp;
    <a href="https://www.rust-lang.org/community"><img src="https://img.shields.io/badge/Rust_Community%20-Join_us-brightgreen?style=plastic&logo=rust"></a>
</p>
<p align="center">
    <a href="https://github.com/KipData/KipSQL/actions/workflows/ci.yml"><img src="https://github.com/KipData/KipSQL/actions/workflows/ci.yml/badge.svg" alt="CI"></img></a>
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
Tips: Install rust toolchain and llvm first.

Clone the repository
``` shell
git clone https://github.com/KipData/FnckSQL.git
```

Using FnckSQL in code
```rust
let fnck_sql = DataBaseBuilder::path("./data")
    .build()?;
let tuples = fnck_sql.run("select * from t1")?;
```
Storage Support:
- RocksDB

#### Build From Source
~~~shell
git clone https://github.com/KipData/FnckSQL.git
cd FnckSQL
docker build -t kould23333/fncksql:latest .
~~~

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
- User-Defined Function: `features = ["marcos"]`
```rust
function!(TestFunction::test(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => |v1: ValueRef, v2: ValueRef| {
    let plus_binary_evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Plus)?;
    let value = plus_binary_evaluator.binary_eval(&v1, &v2);

    let plus_unary_evaluator = EvaluatorFactory::unary_create(LogicalType::Integer, UnaryOperator::Minus)?;
    Ok(plus_unary_evaluator.unary_eval(&value))
});

let fnck_sql = DataBaseBuilder::path("./data")
    .register_function(TestFunction::new())
    .build()?;
```
- Optimizer
  - RBO
  - CBO based on RBO(Physical Selection)
- Execute
  - Volcano
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
  - PrimaryKey
  - Unique
  - Normal
  - Composite
- Supports multiple primary key types
  - Tinyint
  - UTinyint
  - Smallint
  - USmallint
  - Integer
  - UInteger
  - Bigint
  - UBigint
  - Char
  - Varchar
- DDL
  - Begin (Server only)
  - Commit (Server only)
  - Rollback (Server only)
  - Create
    - [x] Table
    - [x] Index: Unique\Normal\Composite
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
  - [x] SubQuery[select/from/where]
  - [x] Join: Inner/Left/Right/Full/Cross (Natural\Using)
  - [x] Group By
  - [x] Having
  - [x] Order By
  - [x] Limit
  - [x] Show Tables
  - [x] Explain
  - [x] Describe
  - [x] Union
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
  - Char
  - Varchar
  - Date
  - DateTime
  - Time
  - Tuple

## Roadmap
- SQL 2016

## License

FnckSQL uses the [Apache 2.0 license][1] to strike a balance between
open contributions and allowing you to use the software however you want.

[1]: <https://github.com/KipData/KipSQL/blob/main/LICENSE>

## Contributors
[![](https://opencollective.com/fncksql/contributors.svg?width=890&button=false)](https://github.com/KipData/FnckSQL/graphs/contributors)

## Thanks For
- [Fedomn/sqlrs](https://github.com/Fedomn/sqlrs): Main reference materials, Optimizer and Executor all refer to the design of sqlrs
- [systemxlabs/bustubx](https://github.com/systemxlabs/bustubx)
- [duckdb/duckdb](https://github.com/duckdb/duckdb)

<pre align="center">
Built by @KipData

██╗  ██╗██╗██████╗ ███████╗ ██████╗ ██╗
██║ ██╔╝██║██╔══██╗██╔════╝██╔═══██╗██║
█████╔╝ ██║██████╔╝███████╗██║   ██║██║
██╔═██╗ ██║██╔═══╝ ╚════██║██║▄▄ ██║██║
     ██║  ██╗██║██║     ███████║╚██████╔╝███████╗
     ╚═╝  ╚═╝╚═╝╚═╝     ╚══════╝ ╚══▀▀═╝ ╚══════╝
-----------------------------------
Embedded SQL DBMS
</pre>
<h3 align="center">
    The Lightweight Embedded OLTP Open-source Database
</h3>

<p align="center">
    &nbsp;
    <a href="https://github.com/KipData/KipSQL/actions/workflows/ci.yml"><img src="https://github.com/KipData/KipSQL/actions/workflows/ci.yml/badge.svg" alt="CI"></img></a>
    &nbsp;
    <a href="https://github.com/KipData/KipSQL/blob/main/LICENSE"><img src="https://img.shields.io/github/license/KipData/KipSQL"></a>
    &nbsp;
    <a href="https://www.rust-lang.org/community"><img src="https://img.shields.io/badge/Rust_Community%20-Join_us-brightgreen?style=plastic&logo=rust"></a>
</p>
<p align="center">
  <a href="https://github.com/KipData/KipSQL" target="_blank">
    <img src="https://img.shields.io/github/stars/KipData/KipSQL.svg?style=social" alt="github star"/>
    <img src="https://img.shields.io/github/forks/KipData/KipSQL.svg?style=social" alt="github fork"/>
  </a>
</p>

### What is KipSQL

KipSQL is designed to allow small Rust projects to reduce external dependencies and get rid of heavy database maintenance, 
so that the Rust application itself can provide SQL storage capabilities.

Welcome to our WebSite, Power By KipSQL: **http://www.kipdata.site/**

### Quick Started
Clone the repository
``` shell
git clone https://github.com/KipData/KipSQL.git
```

Install rust toolchain first.
```
cargo run
```
Example
```sql
create table blog (id int primary key, title varchar unique);

insert into blog (id, title) values (0, 'KipSQL'), (1, 'KipDB'), (2, 'KipBlog');

update blog set title = 'KipData' where id = 2;

select * from blog order by title desc nulls first

select count(distinct id) from blog;

delete from blog where title like 'Kip%';

truncate table blog;

drop table blog;
```
Using KipSQL in code
```rust
let kipsql = Database::with_kipdb("./data").await?;

let tupes = db.run("select * from t1").await?;
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
- SQL field options
  - not null
  - null
  - unique
  - primary key
- SQL where options
  - is null
  - is not null
  - like
  - not like
  - in
  - not in
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
  - Create
    - [x] Table
    - [ ] Index
  - Drop
    - [x] Table
    - [ ] Index
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

KipSQL uses the [Apache 2.0 license][1] to strike a balance between
open contributions and allowing you to use the software however you want.

[1]: <https://github.com/KipData/KipSQL/blob/main/LICENSE>

### Thanks For
- [Fedomn/sqlrs](https://github.com/Fedomn/sqlrs): Main reference materials, Optimizer and Executor all refer to the design of sqlrs
- [systemxlabs/bustubx](https://github.com/systemxlabs/bustubx)
- [duckdb/duckdb](https://github.com/duckdb/duckdb)

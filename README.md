# KipSQL 

> Lightweight SQL calculation engine, as the SQL layer of KipDB, implemented with TalentPlan's TinySQL as the reference standard

### Architecture
![architecture](./static/images/architecture.png)

### Get Started
Install rust toolchain first.
```
cargo run
```
test command
```sql
create table t1 (a int, b int);

insert into t1 (a, b) values (1, 1), (5, 3), (5, 2);

update t1 set a = 0 where b > 1;

delete from t1 where b > 1;

select * from t1;

select * from t1 order by a asc nulls first

select count(distinct a) from t1;

truncate table t1;

drop table t1;
```
Using KipSQL in code
```rust
let kipsql = Database::with_kipdb("./data").await?;

let tupes = db.run("select * from t1").await?;
```
Storage Support:
- KipDB
- Memory

![demo](./static/images/demo.png)

### Features
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
  - [x] Where
  - [x] Distinct
  - [x] Alias
  - [x] Aggregation: count()/sum()/avg()/min()/max()
  - [ ] Subquery
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
- Optimizer rules
  - Limit Project Transpose
  - Eliminate Limits
  - Push Limit Through Join
  - Push Limit Into Scan
  - Combine Filters
  - Column Pruning
  - Collapse Project

### Thanks For
- [Fedomn/sqlrs](https://github.com/Fedomn/sqlrs): 主要参考资料，Optimizer、Executor均参考自sqlrs的设计
- [systemxlabs/tinysql](https://github.com/systemxlabs/tinysql)

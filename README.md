# KipSQL 

> build the SQL layer of KipDB database.


> 目前处于探索阶段
轻量级SQL计算引擎，作为KipDB的SQL层，以TalentPlan的TinySQL作为参考标准进行实现

## Example
```rust
let kipsql = Database::new_on_mem();

let _ = kipsql.run("create table t1 (a int, b int)").await?;
let _ = kipsql.run("create table t2 (c int, d int)").await?;
let _ = kipsql.run("insert into t1 (b, a) values (1, 1), (3, 3), (5, 4)").await?;
let _ = kipsql.run("insert into t2 (d, c) values (1, 2), (2, 3), (5, 6)").await?;

println!("full t1:");
let vec_batch_full_fields_t1 = kipsql.run("select * from t1").await?;
print_batches(&vec_batch_full_fields_t1)?;

println!("full t2:");
let vec_batch_full_fields_t2 = kipsql.run("select * from t2").await?;
print_batches(&vec_batch_full_fields_t2)?;

println!("projection_and_filter:");
let vec_batch_projection_a = kipsql.run("select a from t1 where a <= b").await?;
print_batches(&vec_batch_projection_a)?;

println!("projection_and_sort:");
let vec_batch_projection_a = kipsql.run("select a from t1 order by a").await?;
print_batches(&vec_batch_projection_a)?;

println!("limit:");
let vec_batch_limit=kipsql.run("select * from t1 limit 1 offset 1").await?;
print_batches(&vec_batch_limit)?;

println!("inner join:");
let vec_batch_inner_join = kipsql.run("select * from t1 inner join t2 on a = c").await?;
print_batches(&vec_batch_inner_join)?;

println!("left join:");
let vec_batch_left_join = kipsql.run("select * from t1 left join t2 on a = c").await?;
print_batches(&vec_batch_left_join)?;

println!("right join:");
let vec_batch_right_join = kipsql.run("select * from t1 right join t2 on a = c and a > 1").await?;
print_batches(&vec_batch_right_join)?;

println!("full join:");
let vec_batch_full_join = kipsql.run("select d, b from t1 full join t2 on a = c and a > 1").await?;
print_batches(&vec_batch_full_join)?;            
```

### Features
- Select
  - Filter
  - Limit
  - Sort
  - Projection
  - TableScan
  - Join (HashJoin)
    - Inner
    - Left
    - Right
    - Full 
- Insert
- CreateTable

### Thanks For
- [sqlrs](https://github.com/Fedomn/sqlrs): 主要参考资料，Optimizer、Executor均参考自sqlrs的设计

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
    SQL as a Function for Rust
</h3>

<p align="center">
    <a href="https://summer-ospp.ac.cn/org/orgdetail/0b09d23d-2510-4537-aa9d-45158bb6bdc2"><img src="https://img.shields.io/badge/OSPP-KipData-3DA639?logo=opensourceinitiative"></a>
    <a href="https://github.com/KipData/FnckSQL/blob/main/LICENSE"><img src="https://img.shields.io/github/license/KipData/FnckSQL"></a>
    &nbsp;
    <a href="https://www.rust-lang.org/community"><img src="https://img.shields.io/badge/Rust_Community%20-Join_us-brightgreen?style=plastic&logo=rust"></a>
</p>
<p align="center">
    <a href="https://github.com/KipData/FnckSQL/actions/workflows/ci.yml"><img src="https://github.com/KipData/FnckSQL/actions/workflows/ci.yml/badge.svg" alt="CI"></img></a>
    <a href="https://crates.io/crates/fnck_sql/"><img src="https://img.shields.io/crates/v/fnck_sql.svg"></a>
    <a href="https://github.com/KipData/FnckSQL" target="_blank">
    <img src="https://img.shields.io/github/stars/KipData/FnckSQL.svg?style=social" alt="github star"/>
    <img src="https://img.shields.io/github/forks/KipData/FnckSQL.svg?style=social" alt="github fork"/>
  </a>
</p>

## Introduction
**FnckSQL** is a lightweight embedded database inspired by **MyRocks** and **SQLite** and completely coded in Rust. It aims to provide a more user-friendly, lightweight, and low-loss RDBMS for Rust programming so that the APP does not rely on other complex components. can perform complex relational data operations

## Key Features
- A lightweight embedded SQL database fully rewritten in Rust
- Higher write speed, more user-friendly API
- All metadata and actual data in KV Storage, and there is no state component (e.g. system table) in the middle
- Supports extending storage for customized workloads
- Supports most of the SQL 2016 syntax

#### 👉[check more](docs/features.md)

## Examples

```rust
let fnck_sql = DataBaseBuilder::path("./data").build()?;

fnck_sql
    .run("create table if not exists t1 (c1 int primary key, c2 int)")?
    .done()?;
fnck_sql
    .run("insert into t1 values(0, 0), (1, 1)")?
    .done()?;

for tuple in fnck_sql.run("select * from t1")? {
    println!("{:?}", tuple?);
}
```

👉**more examples**
- [hello_word](examples/hello_world.rs)
- [transaction](examples/transaction.rs)

## TPC-C
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: TPC-C currently only supports single thread
```shell
<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.004)
     Payment : 0.001  (0.025)
Order-Status : 0.053  (0.175)
    Delivery : 0.022  (0.027)
 Stock-Level : 0.003  (0.019)
<TpmC>
7815 tpmC
```
#### 👉[check more](tpcc/README.md)

## Roadmap
- Get [SQL 2016](https://github.com/KipData/FnckSQL/issues/130) mostly supported
- LLVM JIT: [Perf: TPCC](https://github.com/KipData/FnckSQL/issues/247)

## License

FnckSQL uses the [Apache 2.0 license][1] to strike a balance between
open contributions and allowing you to use the software however you want.

[1]: <https://github.com/KipData/FnckSQL/blob/main/LICENSE>

## Contributors
[![](https://opencollective.com/fncksql/contributors.svg?width=890&button=false)](https://github.com/KipData/FnckSQL/graphs/contributors)

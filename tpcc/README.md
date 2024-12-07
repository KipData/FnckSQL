# TPCC on FnckSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- YMTC PC411-1024GB-B
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 80029  lt: 0  fl: 821
|Payment| sc: 80005  lt: 0  fl: 0
|Order-Status| sc: 8001  lt: 0  fl: 412
|Delivery| sc: 8001  lt: 0  fl: 0
|Stock-Level| sc: 8001  lt: 0  fl: 0
in 720 sec.
<Constraint Check> (all must be [OK])
[transaction percentage]
   Payment: 43.0% (>=43.0%)  [Ok]
   Order-Status: 4.0% (>=4.0%)  [Ok]
   Delivery: 4.0% (>=4.0%)  [Ok]
   Stock-Level: 4.0% (>=4.0%)  [Ok]
[response time (at least 90%% passed)]
   New-Order: 100.0  [OK]
   Payment: 100.0  [OK]
   Order-Status: 100.0  [OK]
   Delivery: 100.0  [OK]
   Stock-Level: 100.0  [OK]
   New-Order Total: 80029
   Payment Total: 80005
   Order-Status Total: 8001
   Delivery Total: 8001
   Stock-Level Total: 8001

<90th Percentile RT (MaxRT)>
   New-Order : 0.003  (0.006)
     Payment : 0.001  (0.003)
Order-Status : 0.062  (0.188)
    Delivery : 0.022  (0.052)
 Stock-Level : 0.004  (0.006)
<TpmC>
6669 Tpmc

```

## Explain
run `cargo test -p tpcc explain_tpcc -- --ignored` to explain tpcc statements

Tips: after TPCC loaded tables

## Refer to
- https://github.com/AgilData/tpcc
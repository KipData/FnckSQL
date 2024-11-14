# TPCC on FnckSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- YMTC PC411-1024GB-B
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 1084  lt: 0  fl: 11
|Payment| sc: 1062  lt: 0  fl: 0
|Order-Status| sc: 102  lt: 4  fl: 36
|Delivery| sc: 107  lt: 0  fl: 0
|Stock-Level| sc: 106  lt: 0  fl: 0
in 723 sec.
<Constraint Check> (all must be [OK])
[transaction percentage]
   Payment: 43.0% (>=43.0%)  [Ok]
   Order-Status: 4.0% (>=4.0%)  [Ok]
   Delivery: 4.0% (>=4.0%)  [Ok]
   Stock-Level: 4.0% (>=4.0%)  [Ok]
[response time (at least 90%% passed)]
   New-Order: 100.0  [OK]
   Payment: 100.0  [OK]
   Order-Status: 96.2  [OK]
   Delivery: 100.0  [OK]
   Stock-Level: 100.0  [OK]
   New-Order Total: 1084
   Payment Total: 1062
   Order-Status Total: 106
   Delivery Total: 107
   Stock-Level Total: 106

<90th Percentile RT (MaxRT)>
   New-Order : 0.005  (0.007)
     Payment : 0.084  (0.141)
Order-Status : 0.492  (0.575)
    Delivery : 6.109  (6.473)
 Stock-Level : 0.001  (0.001)
<TpmC>
89.9205557572134 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
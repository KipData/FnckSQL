# TPCC on FnckSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- YMTC PC411-1024GB-B
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 1182  lt: 0  fl: 13
|Payment| sc: 1155  lt: 0  fl: 0
|Order-Status| sc: 115  lt: 1  fl: 29
|Delivery| sc: 114  lt: 2  fl: 0
|Stock-Level| sc: 115  lt: 0  fl: 0
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
   Order-Status: 99.1  [OK]
   Delivery: 98.3  [OK]
   Stock-Level: 100.0  [OK]
   New-Order Total: 1182
   Payment Total: 1155
   Order-Status Total: 116
   Delivery Total: 116
   Stock-Level Total: 115

<90th Percentile RT (MaxRT)>
   New-Order : 0.003  (0.011)
     Payment : 0.078  (0.470)
Order-Status : 0.227  (0.240)
    Delivery : 5.439  (27.702)
 Stock-Level : 0.001  (0.001)
<TpmC>
98 Tpmc
```

## Refer to
- https://github.com/AgilData/tpcc
# TPCC on FnckSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- YMTC PC411-1024GB-B
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 90803  lt: 0  fl: 906
|Payment| sc: 90783  lt: 0  fl: 0
|Order-Status| sc: 9079  lt: 0  fl: 434
|Delivery| sc: 9078  lt: 0  fl: 0
|Stock-Level| sc: 9078  lt: 0  fl: 0
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
   New-Order Total: 90803
   Payment Total: 90783
   Order-Status Total: 9079
   Delivery Total: 9078
   Stock-Level Total: 9078


<RT Histogram>

1.New-Order

0.001,   8110
0.002,  71039
0.003,  11477
0.004,     35
0.005,      2

2.Payment

0.001,  85279
0.002,   5434
0.003,     15
0.004,      1

3.Order-Status

0.013,     11
0.014,     62
0.015,    161
0.016,    248
0.017,    244
0.018,    201
0.019,    171
0.020,    157
0.021,    179
0.022,    202
0.023,    176
0.024,    262
0.025,    304
0.026,    315
0.027,    234
0.028,    166
0.029,    171
0.030,    104
0.031,    121
0.032,    233
0.033,    284
0.034,    305
0.035,    237
0.036,    209
0.037,    174
0.038,    178
0.039,    140
0.040,    144
0.041,    232
0.042,    242
0.043,    272
0.044,    233
0.045,    212
0.046,    214
0.047,    172
0.048,    158
0.049,    209
0.050,    239
0.051,    314
0.052,    286
0.053,    212
0.054,    173
0.055,    180
0.056,    152
0.057,     71
0.058,     27
0.059,      7
0.060,      5
0.061,      4
0.069,      1
0.071,      1
0.072,      1
0.077,      1
0.079,      1
0.087,      1
0.089,      1
0.099,      1
0.100,      1
0.110,      1

4.Delivery

0.011,      1
0.012,    177
0.013,    721
0.014,    840
0.015,    844
0.016,    898
0.017,   1128
0.018,   1339
0.019,   1123
0.020,    930
0.021,    590
0.022,    305
0.023,    153
0.024,     22
0.025,      4
0.026,      2
0.046,      1

5.Stock-Level

0.001,   1844
0.002,   3495
0.003,   2976
0.004,    604
0.005,     59

<90th Percentile RT (MaxRT)>
   New-Order : 0.003  (0.053)
     Payment : 0.001  (0.011)
Order-Status : 0.052  (0.110)
    Delivery : 0.021  (0.046)
 Stock-Level : 0.003  (0.005)
<TpmC>
7567 Tpmc
```

## Explain
run `cargo test -p tpcc explain_tpcc -- --ignored` to explain tpcc statements

Tips: after TPCC loaded tables

## Refer to
- https://github.com/AgilData/tpcc
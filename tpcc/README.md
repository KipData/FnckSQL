# TPCC on FnckSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- YMTC PC411-1024GB-B
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 89485  lt: 0  fl: 900
|Payment| sc: 89460  lt: 0  fl: 0
|Order-Status| sc: 8947  lt: 0  fl: 390
|Delivery| sc: 8947  lt: 0  fl: 0
|Stock-Level| sc: 8946  lt: 0  fl: 0
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
   New-Order Total: 89485
   Payment Total: 89460
   Order-Status Total: 8947
   Delivery Total: 8947
   Stock-Level Total: 8946


<RT Histogram>

1.New-Order

0.001,   5430
0.002,  64752
0.003,  19151
0.004,    107
0.005,      5
0.006,      1

2.Payment

0.001,  83636
0.002,   5649
0.003,      9

3.Order-Status

0.013,      8
0.014,     73
0.015,    169
0.016,    228
0.017,    244
0.018,    206
0.019,    171
0.020,    176
0.021,    182
0.022,    195
0.023,    152
0.024,    206
0.025,    289
0.026,    313
0.027,    255
0.028,    217
0.029,    203
0.030,    160
0.031,    144
0.032,    117
0.033,    204
0.034,    263
0.035,    255
0.036,    242
0.037,    167
0.038,    193
0.039,    164
0.040,    149
0.041,    144
0.042,    214
0.043,    246
0.044,    283
0.045,    223
0.046,    175
0.047,    201
0.048,    157
0.049,    158
0.050,    184
0.051,    219
0.052,    268
0.053,    260
0.054,    257
0.055,    223
0.056,    148
0.057,     81
0.058,     52
0.059,     18
0.060,     13
0.061,      7
0.062,      8
0.063,      2
0.065,      1
0.069,      1
0.070,      1
0.071,      1
0.074,      1
0.082,      1
0.087,      2
0.095,      3
0.100,      1
0.103,      1
0.114,      1
0.128,      1
0.132,      1
0.143,      1
0.145,      1

4.Delivery

0.011,      4
0.012,    246
0.013,    738
0.014,    821
0.015,    898
0.016,   1012
0.017,   1247
0.018,   1234
0.019,   1097
0.020,    697
0.021,    466
0.022,    224
0.023,    108
0.024,     26
0.025,      6
0.030,      1
0.032,      1

5.Stock-Level

0.001,   1466
0.002,   2851
0.003,   3136
0.004,    950
0.005,    381
0.006,     52
0.007,      3

<90th Percentile RT (MaxRT)>
   New-Order : 0.003  (0.031)
     Payment : 0.001  (0.028)
Order-Status : 0.053  (0.144)
    Delivery : 0.020  (0.032)
 Stock-Level : 0.004  (0.007)
<TpmC>
7457 Tpmc

```

## Explain
run `cargo test -p tpcc explain_tpcc -- --ignored` to explain tpcc statements

Tips: after TPCC loaded tables

## Refer to
- https://github.com/AgilData/tpcc
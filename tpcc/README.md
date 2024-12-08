# TPCC on FnckSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- YMTC PC411-1024GB-B
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 88139  lt: 0  fl: 897
|Payment| sc: 88120  lt: 0  fl: 0
|Order-Status| sc: 8812  lt: 0  fl: 388
|Delivery| sc: 8812  lt: 0  fl: 0
|Stock-Level| sc: 8812  lt: 0  fl: 0
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
   New-Order Total: 88139
   Payment Total: 88120
   Order-Status Total: 8812
   Delivery Total: 8812
   Stock-Level Total: 8812


<RT Histogram>

1.New-Order

0.001,   5110
0.002,  63448
0.003,  19415
0.004,     78
0.005,      3
0.006,      1
0.013,      2

2.Payment

0.001,  81269
0.002,   6794
0.003,     12
0.004,      1

3.Order-Status

0.014,     34
0.015,    143
0.016,    207
0.017,    225
0.018,    221
0.019,    196
0.020,    162
0.021,    170
0.022,    166
0.023,    206
0.024,    190
0.025,    134
0.026,    151
0.027,    287
0.028,    274
0.029,    273
0.030,    206
0.031,    169
0.032,    170
0.033,    149
0.034,    136
0.035,    181
0.036,    244
0.037,    295
0.038,    294
0.039,    232
0.040,    201
0.041,    181
0.042,    173
0.043,    165
0.044,    154
0.045,    175
0.046,    267
0.047,    286
0.048,    233
0.049,    190
0.050,    153
0.051,    183
0.052,    199
0.053,    155
0.054,    190
0.055,    237
0.056,    190
0.057,    151
0.058,     82
0.059,     50
0.060,     14
0.061,      5
0.062,      4
0.063,      2
0.064,      2
0.065,      1
0.073,      1
0.075,      1
0.078,      1
0.087,      2
0.102,      2
0.131,      3
0.188,      1

4.Delivery

0.012,     96
0.013,    580
0.014,    786
0.015,    882
0.016,    893
0.017,   1087
0.018,   1200
0.019,   1038
0.020,    842
0.021,    576
0.022,    416
0.023,    247
0.024,     94
0.025,     13
0.027,      1
0.028,      2
0.031,      1
0.034,      1
0.050,      1

5.Stock-Level

0.001,   1299
0.002,   2836
0.003,   3192
0.004,   1150
0.005,    172
0.006,      7

<90th Percentile RT (MaxRT)>
   New-Order : 0.003  (0.012)
     Payment : 0.001  (0.003)
Order-Status : 0.054  (0.188)
    Delivery : 0.021  (0.049)
 Stock-Level : 0.004  (0.006)
<TpmC>
7345 Tpmc

```

## Explain
run `cargo test -p tpcc explain_tpcc -- --ignored` to explain tpcc statements

Tips: after TPCC loaded tables

## Refer to
- https://github.com/AgilData/tpcc
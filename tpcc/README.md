# TPCC on FnckSQL
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- YMTC PC411-1024GB-B
- Tips: TPCC currently only supports single thread
```shell
|New-Order| sc: 93061  lt: 0  fl: 977
|Payment| sc: 93036  lt: 0  fl: 0
|Order-Status| sc: 9304  lt: 0  fl: 420
|Delivery| sc: 9304  lt: 0  fl: 0
|Stock-Level| sc: 9303  lt: 0  fl: 0
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
   New-Order Total: 93061
   Payment Total: 93036
   Order-Status Total: 9304
   Delivery Total: 9304
   Stock-Level Total: 9303


<RT Histogram>

1.New-Order

0.001,  17079
0.002,  73000
0.003,   2935
0.004,     36
0.005,      4
0.006,      1
0.009,      1

2.Payment

0.001,  89591
0.002,   3345
0.003,      7
0.004,      2

3.Order-Status

0.013,     10
0.014,     96
0.015,    173
0.016,    217
0.017,    233
0.018,    190
0.019,    135
0.020,    120
0.021,    149
0.022,    217
0.023,    231
0.024,    283
0.025,    266
0.026,    320
0.027,    221
0.028,    193
0.029,    189
0.030,    179
0.031,    153
0.032,    156
0.033,    193
0.034,    276
0.035,    266
0.036,    210
0.037,    210
0.038,    196
0.039,    149
0.040,    132
0.041,    179
0.042,    210
0.043,    223
0.044,    300
0.045,    232
0.046,    246
0.047,    211
0.048,    173
0.049,    163
0.050,    203
0.051,    260
0.052,    241
0.053,    225
0.054,    213
0.055,    174
0.056,    200
0.057,    175
0.058,    113
0.059,     47
0.060,     24
0.061,      7
0.062,      1
0.063,      3
0.064,      3
0.065,      3
0.066,      1
0.067,      2
0.069,      1
0.082,      1
0.110,      1
0.114,      1
0.124,      1
0.126,      1
0.128,      1
0.143,      1
0.145,      1

4.Delivery

0.011,    122
0.012,    552
0.013,    677
0.014,    771
0.015,    981
0.016,   1203
0.017,   1289
0.018,   1211
0.019,    958
0.020,    639
0.021,    439
0.022,    199
0.023,     53
0.024,      7
0.025,      4
0.026,      3
0.029,      3
0.031,      1
0.034,      1
0.036,      2
0.043,      1
0.056,      1

5.Stock-Level

0.001,   1670
0.002,   3091
0.003,   3008
0.004,    516
0.005,     89
0.006,      2
0.008,      1

<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.122)
     Payment : 0.001  (0.027)
Order-Status : 0.054  (0.144)
    Delivery : 0.020  (0.056)
 Stock-Level : 0.003  (0.015)
<TpmC>
7755 Tpmc
```

## Explain
run `cargo test -p tpcc explain_tpcc -- --ignored` to explain tpcc statements

Tips: after TPCC loaded tables

## Refer to
- https://github.com/AgilData/tpcc
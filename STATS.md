dec4-23
5 workers

Empty command handler
1.6M/sec

Just WAL
80k puts/sec, 534us@P99.9

Just B tree
30k/sec, 177us@P99.9

WAL + B tree
35k/sec, 530us@P99.9

GRPC + empty command handler
18k/sec, 20us@P99.9

GRPC + WAL + B tree
18k/sec, 518us@P99.9
10 workers
17k/sec, 1.6ms@P99.9
2 workers
11k/sec, 303us@P99.9

QUIC + empty command handler
42k/sec, 20us@P99.9

QUIC + WAL
32k/sec, 1ms@P99.9

QUIC + B tree
34k/sec, 538us@P99.9

QUIC + WAL + B Tree
24k/sec, 1.4ms@P99.9

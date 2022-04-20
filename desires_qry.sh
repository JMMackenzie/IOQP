#!/usr/bin/env bash

set -e

# SPATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
D=desires
QUERY=./target/release/query

NUMANODES=$(lscpu | grep '^NUMA node(s):' | awk '{print $3}')
if ((NUMANODES > 1)); then
    echo "warn: found $NUMANODES numa nodes"
    echo 'warn: the numactl command can be specified via `NUMACTL` variable:'
    echo 'warn:     NUMACTL="<numactl command>" ./desires_qry.sh'
    echo
fi

# GOV2: 25,205,179
# MSMARCO: 8,841,823
GOV2_D_10_PERCENT=2520518
MARCO_D_10_PERCENT=884182

mkdir -p $D/log
mkdir -p $D/{gov2,msmarco}/runs

ioqp_qry() {
    $NUMACTL $QUERY \
        -i $1 \
        -q $2 \
        -o $3 \
        -k $4 \
        -m $5 \
        --warmup \
        > $6
    # drop unknown query terms from output
    sed -i '/^unknown/d' $6
}

# msmarco
for K in 10 100 1000; do
for MODE in fixed-10 fixed-100 fixed-1000 fixed-$MARCO_D_10_PERCENT fraction-1; do

# msmarco, deepct
NAME=deepct.k${K}.${MODE}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-deepct.8.ioqp.idx
QRYSET=$D/msmarco/queries/deepct.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K $MODE $LOG

# msmarco, deepimpact
NAME=deepimpact.k${K}.${MODE}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-deepimpact.ioqp.idx
QRYSET=$D/msmarco/queries/deepimpact.query
ioqp_qry $INDEX $QRYSET $RUN $K $MODE $LOG

# msmarco, doct5query
NAME=doct5query.k${K}.${MODE}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-doct5query.8.ioqp.idx
QRYSET=$D/msmarco/queries/doct5query.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K $MODE $LOG

# msmarco, original
NAME=original.k${K}.${MODE}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-original.8.ioqp.idx
QRYSET=$D/msmarco/queries/original.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K $MODE $LOG

# msmarco, spladev2
NAME=spladev2.k${K}.${MODE}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-spladev2.ioqp.idx
QRYSET=$D/msmarco/queries/spladev2.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K $MODE $LOG

# msmarco, unicoil-tilde
NAME=unicoil-tilde.k${K}.${MODE}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-unicoil-tilde.ioqp.idx
QRYSET=$D/msmarco/queries/unicoil-tilde.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K $MODE $LOG

done
done


# gov2
for K in 10 100 1000; do
for MODE in fixed-10 fixed-100 fixed-1000 fixed-$GOV2_D_10_PERCENT fraction-1; do

# gov2, terabyte
NAME=gov2-tbq.k${K}.${MODE}
RUN=$D/gov2/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/gov2/indexes/bp-gov2.8.ioqp.idx
QRYSET=$D/gov2/queries/gov2.queries
ioqp_qry $INDEX $QRYSET $RUN $K $MODE $LOG

# gov2, mqt
NAME=gov2-mqt.k${K}.${MODE}
RUN=$D/gov2/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/gov2/indexes/bp-gov2.8.ioqp.idx
QRYSET=$D/gov2/queries/mqt.queries
ioqp_qry $INDEX $QRYSET $RUN $K $MODE $LOG

done
done

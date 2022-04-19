#!/usr/bin/env bash

# TODO: numactl

set -ex

# SPATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
D=desires
QUERY=./target/release/query

mkdir -p $D/log
mkdir -p $D/{gov2,msmarco}/runs

ioqp_qry() {
    $QUERY \
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

for K in 10 100 1000; do
for RHO in 10 100 1000; do
    # if ((K > RHO)); then
    #     continue
    # fi
    # echo $K $RHO

# msmarco, deepct
NAME=deepct.k${K}.fixed${RHO}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-deepct.8.ioqp.idx
QRYSET=$D/msmarco/queries/deepct.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K fixed-$RHO $LOG

# msmarco, deepimpact
NAME=deepimpact.k${K}.fixed${RHO}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-deepimpact.ioqp.idx
QRYSET=$D/msmarco/queries/deepimpact.query
ioqp_qry $INDEX $QRYSET $RUN $K fixed-$RHO $LOG

# msmarco, doct5query
NAME=doct5query.k${K}.fixed${RHO}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-doct5query.8.ioqp.idx
QRYSET=$D/msmarco/queries/doct5query.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K fixed-$RHO $LOG

# msmarco, original
NAME=original.k${K}.fixed${RHO}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-original.8.ioqp.idx
QRYSET=$D/msmarco/queries/original.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K fixed-$RHO $LOG

# msmarco, spladev2
NAME=spladev2.k${K}.fixed${RHO}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-spladev2.ioqp.idx
QRYSET=$D/msmarco/queries/spladev2.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K fixed-$RHO $LOG

# msmarco, unicoil-tilde
NAME=unicoil-tilde.k${K}.fixed${RHO}
RUN=$D/msmarco/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/msmarco/indexes/bp-unicoil-tilde.ioqp.idx
QRYSET=$D/msmarco/queries/unicoil-tilde.dev.query
ioqp_qry $INDEX $QRYSET $RUN $K fixed-$RHO $LOG

# gov2, terabyte
NAME=gov2-tbq.k${K}.fixed${RHO}
RUN=$D/gov2/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/gov2/indexes/bp-gov2.8.ioqp.idx
QRYSET=$D/gov2/queries/gov2.queries
ioqp_qry $INDEX $QRYSET $RUN $K fixed-$RHO $LOG

# gov2, mqt
NAME=gov2-mqt.k${K}.fixed${RHO}
RUN=$D/gov2/runs/${NAME}.run
LOG=$D/log/${NAME}.log
INDEX=$D/gov2/indexes/bp-gov2.8.ioqp.idx
QRYSET=$D/gov2/queries/mqt.queries
ioqp_qry $INDEX $QRYSET $RUN $K fixed-$RHO $LOG

done
done

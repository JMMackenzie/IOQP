#!/usr/bin/env bash

set -ex

D=data
CREATE=./target/release/create

# build indexes
mkdir -p $D/{gov2,msmarco}/indexes

# pre-quantized
for i in \
    $D/msmarco/ciff/bp-deepimpact.ciff \
    $D/msmarco/ciff/bp-spladev2.ciff \
    $D/msmarco/ciff/bp-unicoil-tilde.ciff
do
    name=$(basename ${i%.ciff})
    path=$(dirname $(dirname $i))
    index="$path/indexes/$name.ioqp.idx"
    echo $index
    $CREATE -i $i -o $index
done
# need-quantize
$CREATE -i $D/gov2/ciff/bp-gov2.ciff \
    -o $D/gov2/indexes/bp-gov2.8.ioqp.idx \
    --quantize \
    --quant-bits 8
$CREATE -i $D/msmarco/ciff/bp-deepct.ciff \
    -o $D/msmarco/indexes/bp-deepct.8.ioqp.idx \
    --quantize \
    --quant-bits 8 \
    --bm25-k1 8.0 \
    --bm25-b 0.9
$CREATE -i $D/msmarco/ciff/bp-doct5query.ciff \
    -o $D/msmarco/indexes/bp-doct5query.8.ioqp.idx \
    --quantize \
    --quant-bits 8 \
    --bm25-k1 0.82 \
    --bm25-b 0.68
$CREATE -i $D/msmarco/ciff/bp-original.ciff \
    -o $D/msmarco/indexes/bp-original.8.ioqp.idx \
    --quantize \
    --quant-bits 8 \
    --bm25-k1 0.82 \
    --bm25-b 0.68

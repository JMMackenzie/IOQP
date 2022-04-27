#!/usr/bin/env bash

set -ex

# SPATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
D=desires
STEP1=$D/.step1_download
STEP2=$D/.step2_extract
STEP3=$D/.step3_index
CREATE=./target/release/create

# download
if [ ! -f "$STEP1" ]; then
    wget https://cloudstor.aarnet.edu.au/plus/s/AYX0o3PB0fXtQ7p/download -O ioqp.zip
    unzip ioqp.zip -x ioqp/ccnews/*
    mv ioqp $D
    # rm -v ioqp.zip
    touch $STEP1
fi

# extract ciff
if [ ! -f "$STEP2" ]; then
    for i in \
        $D/gov2/ciff/bp-gov2.ciff.gz \
        $D/msmarco/ciff/bp-deepct.ciff.gz \
        $D/msmarco/ciff/bp-deepimpact.ciff.gz \
        $D/msmarco/ciff/bp-doct5query.ciff.gz \
        $D/msmarco/ciff/bp-original.ciff.gz \
        $D/msmarco/ciff/bp-spladev2.ciff.gz \
        $D/msmarco/ciff/bp-unicoil-tilde.ciff.gz
    do
        # echo $i ${i%.gz}
        gunzip -v $i
    done
    touch $STEP2
fi

# build indexes
if [ ! -f "$STEP3" ]; then
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

    touch $STEP3
fi

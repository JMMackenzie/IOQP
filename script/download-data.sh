#!/usr/bin/env bash

set -ex

D=data

# download
wget https://cloudstor.aarnet.edu.au/plus/s/AYX0o3PB0fXtQ7p/download -O ioqp.zip
unzip ioqp.zip -x ioqp/ccnews/*
mv ioqp $D
rm -v ioqp.zip

# extract ciff
for i in \
    $D/gov2/ciff/bp-gov2.ciff.gz \
    $D/msmarco/ciff/bp-deepct.ciff.gz \
    $D/msmarco/ciff/bp-deepimpact.ciff.gz \
    $D/msmarco/ciff/bp-doct5query.ciff.gz \
    $D/msmarco/ciff/bp-original.ciff.gz \
    $D/msmarco/ciff/bp-spladev2.ciff.gz \
    $D/msmarco/ciff/bp-unicoil-tilde.ciff.gz
do
    gunzip -v $i
done

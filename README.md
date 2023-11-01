# IOQP
An Impact Ordered Query Processor.


## Credits

IOQP is loosely based on the [JASS](https://github.com/lintool/JASS) and [JASSv2](https://github.com/andrewtrotman/JASSv2) search systems.

IOQP makes use of open-source Rust code from the [faster-graph-bisection](https://github.com/mpetri/faster-graph-bisection) library.

## Citation Information

If you use this code in your own work or research, please consider citing our
work:
```
@inproceedings{mpg22-desires,
 title = {IOQP: A simple Impact-Ordered Query Processor written in Rust},
 author = {J. Mackenzie and M. Petri and L. Gallagher},
 booktitle = {Proc. DESIRES},
 year = {2022},
 pages = {22--34},
}
```

## Build instructions

You can build the code using cargo:

```
cargo build --release
```

## Running Experiments

Use the following scripts to run the Gov2 and MS MARCO experiments from the paper:

```
./script/download-data.sh
./script/build-indexes.sh
./script/run-queries.sh
```

The run files are located in `data/gov2/runs` and `data/msmarco/runs`. Timing
results can be found in `data/log`.

### Throughput Experiments

Here we give an example of the multi-threaded throughput experiments for Gov2.
In the following we first start the server to host an index and listen for
incoming requests. Then we run the load generator to simulate a workload of
incoming queries.

1. Starting the server

```
$ ./target/release/serve --max-blocking-threads 16 --index data/gov2/indexes/bp-gov2.8.ioqp.idx 
2022-08-17T01:19:18.406794Z  INFO serve: args = Args { index: "data/gov2/indexes/bp-gov2.8.ioqp.idx", port: 3000, max_blocking_threads: 16 }
2022-08-17T01:19:18.406844Z  INFO serve: loading index from file data/gov2/indexes/bp-gov2.8.ioqp.idx
2022-08-17T01:21:22.277424Z  INFO serve: start http endpoint at 0.0.0.0:3000
```

2. Run the load generator. In this example we are using exhaustive processing
   with 10 incoming queries per second.

```
$ ./target/release/load_gen --k 1000 --mode fraction-1 --queries data/gov2/queries/mqt.queries --tps 10
2022-08-17T01:29:59.276151Z  INFO load_gen: read queries = 59986
2022-08-17T01:34:59.322531Z  INFO load_gen: ======= Server Time =======
2022-08-17T01:34:59.322544Z  INFO load_gen: # of samples: 2009
2022-08-17T01:34:59.322548Z  INFO load_gen:   50'th percntl.: 30500µs
2022-08-17T01:34:59.322553Z  INFO load_gen:   90'th percntl.: 69395µs
2022-08-17T01:34:59.322558Z  INFO load_gen:   99'th percntl.: 116047µs
2022-08-17T01:34:59.322562Z  INFO load_gen: 99.9'th percntl.: 149946µs
2022-08-17T01:34:59.322565Z  INFO load_gen:             max.: 164049µs
2022-08-17T01:34:59.322578Z  INFO load_gen:        mean time: 36120.8µs
2022-08-17T01:34:59.322648Z  INFO load_gen: ======= User Time =======
2022-08-17T01:34:59.322651Z  INFO load_gen: # of samples: 2009
2022-08-17T01:34:59.322655Z  INFO load_gen:   50'th percntl.: 31352µs
2022-08-17T01:34:59.322660Z  INFO load_gen:   90'th percntl.: 70331µs
2022-08-17T01:34:59.322663Z  INFO load_gen:   99'th percntl.: 116714µs
2022-08-17T01:34:59.322666Z  INFO load_gen: 99.9'th percntl.: 150643µs
2022-08-17T01:34:59.322669Z  INFO load_gen:             max.: 164848µs
2022-08-17T01:34:59.322673Z  INFO load_gen:        mean time: 36953.0µs
```

## Data

CIFF files and queries can be found on [AARNET CloudStor.](https://cloudstor.aarnet.edu.au/plus/s/AYX0o3PB0fXtQ7p)
Use the `script/download-data.sh` script to fetch the data used in the paper, or you can download it manually:

`wget "https://cloudstor.aarnet.edu.au/plus/s/AYX0o3PB0fXtQ7p/download" -O ioqp.zip`

Or just bits and pieces:

`wget "https://cloudstor.aarnet.edu.au/plus/s/AYX0o3PB0fXtQ7p/download?path=gov2/queries" -O gov2-queries`

## Indexing

Use the `script/build-indexes.sh` to build the indexes from the paper.

Index a CIFF file and perform quantization:

```
./target/release/create \
    --input data/gov2/ciff/bp-gov2.ciff \
    --output data/gov2/indexes/bp-gov2.8.ioqp.idx \
    --quantize \
    --quant-bits 8 \
    --bm25-k1 0.9 \
    --bm25-b 0.4
```

Index a CIFF file that is already quantized:

```
./target/release/create \
    --input data/msmarco/ciff/bp-spladev2.ciff \
    --output data/msmarco/indexes/bp-spladev2.ioqp.idx
```

## Query processing

Use the `script/run-queries.sh` to run the queries from the paper.

Query processing with exhaustive mode:

```
./target/release/query \
    --index data/gov2/bp-gov2.8.ioqp.idx \
    --queries data/gov2/queries/gov2.queries \
    --output data/gov2/run/gov2.run \
    --k 1000 \
    --mode fraction-1 \
    --warmup
```

Query processing with fixed budget:

```
./target/release/query \
    --index data/gov2/bp-gov2.8.ioqp.idx \
    --queries data/gov2/queries/gov2.queries \
    --output data/gov2/run/gov2.run \
    --k 1000 \
    --mode fixed-10000 \
    --warmup
```

Query processing with query term weights:

```
./target/release/query \
    --index data/msmarco/bp-spladev2.ioqp.idx \
    --queries data/msmarco/queries/spladev2.dev.query \
    --output data/msmarco/run/spladev2.run \
    --k 1000 \
    --weighted
```

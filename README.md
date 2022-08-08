# IOQP
An Impact Ordered Query Processor.


## Credits

IOQP is loosely based on the [JASS](https://github.com/lintool/JASS) and [JASSv2](https://github.com/andrewtrotman/JASSv2) search systems.

IOQP makes use of open-source Rust code from the [faster-graph-bisection](https://github.com/mpetri/faster-graph-bisection) library.

## Data

Some data can be found on [AARNET CloudStor.](https://cloudstor.aarnet.edu.au/plus/s/AYX0o3PB0fXtQ7p)

You can download all of the data:

`wget "https://cloudstor.aarnet.edu.au/plus/s/AYX0o3PB0fXtQ7p/download" -O ioqp.zip`

Or just bits and pieces:

`wget "https://cloudstor.aarnet.edu.au/plus/s/AYX0o3PB0fXtQ7p/download?path=gov2/queries" -O gov2-queries`

## Requirements

- Requires `protoc` to be installed which can be done on ubuntu using the command:

```
sudo apt install protobuf-compiler
```


# A tiny tool to clear Aerospike sets

Of course, there is official tool: https://github.com/aerospike/delete-set

But this one is written in Go and much faster.

## How to use it?

Install it with:
```
go install github.com/elgris/ascleaner
```

Run it like this:
```
./aerospike-cleaner --help
Usage of ./aerospike-cleaner:
  -b int
        Size of the buffer to pre-load keys before deleting (default 25000)
  -h string
        Comma-separated list of aerospike hosts (default "localhost:3000")
  -n string
        Data namespace
  -s string
        Comma-separated list of set names to erase
  -t duration
        Connection timeout (default 5s)
```

## Examples
That's how you can clear all the data from namespace `test`:
```
./aerospike-cleaner -h docker:3000 -n test
```

That's how you can clear sets `foo` and `bar` from namespace `test`:
```
./aerospike-cleaner -h docker:3000 -n test -s foo,bar
```
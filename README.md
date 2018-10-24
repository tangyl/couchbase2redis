
# couchbase2redis - Redis proxy server for couchbase

## Requirements

redis value must be json serializable bytes.

## Usage

```
Usage of ./couchbase2redis:
  -bucket string
    	bucket name
  -cacheTTL int
    	cache ttl time (default 300)
  -couchbase string
    	couchbase server (default "couchbase://localhost")
  -listen string
    	listen ip and port (default ":6380")
  -password string
    	password
  -useCache
    	use cache
  -user string
    	user name
```

## Implemented redis commands

* GET
* SET
* DEL
* MGET
* MSET
* EXPIRE
* TTL

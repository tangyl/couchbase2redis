package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	gocache "github.com/patrickmn/go-cache"

	// bigcache "github.com/allegro/bigcache"
	"github.com/tidwall/redcon"
	gocb "gopkg.in/couchbase/gocb.v1"
)

// Proxy instance
type Proxy struct {
	bucket *gocb.Bucket
	listen string
	cache  *gocache.Cache
}

// NewProxy create proxy instance
func NewProxy(listen string, couchbase string, user string, password string, bucket string, useCache bool, cacheTTL int) (*Proxy, error) {
	var err error
	cluster, err := gocb.Connect(couchbase)
	if err != nil {
		return nil, err
	}
	cluster.Authenticate(gocb.PasswordAuthenticator{
		Username: user,
		Password: password})

	bkt, err := cluster.OpenBucket(bucket, "")

	if err != nil {
		return nil, err
	}

	log.Printf("connected to %s", couchbase)

	var cache *gocache.Cache

	if useCache {

		ttl := time.Duration(cacheTTL) * time.Second
		log.Printf("cache enabled with TTL=%s", ttl)

		cache = gocache.New(ttl, ttl)

		/*
			ttl := time.Duration(cacheTTL) * time.Second

			config := bigcache.Config{
				Shards:             1024,
				LifeWindow:         ttl,
				MaxEntriesInWindow: 100 * cacheTTL,
				MaxEntrySize:       1024,
				Verbose:            true,
				HardMaxCacheSize:   4096,
				OnRemove:           nil,
				OnRemoveWithReason: nil}

			cache, _ = bigcache.NewBigCache(config)

			if err != nil {
				log.Printf("init bigcache failed")
				return nil, err
			}
		*/
	}

	return &Proxy{bucket: bkt, listen: listen, cache: cache}, nil
}

func (proxy *Proxy) get(key []byte) ([]byte, bool, error) {
	if proxy.cache == nil {
		return proxy.rawGet(key)
	}

	/*
		bytes, err := proxy.cache.Get(string(key))

		if err != nil {
			if _, ok := err.(*bigcache.EntryNotFoundError); ok {
				rawBytes, succ, err := proxy.rawGet(key)
				if err == nil && succ {
					proxy.cache.Set(string(key), rawBytes)
				} else if err == nil {
					proxy.cache.Set(string(key), nil)
				}
				return rawBytes, succ, err
			}
			return nil, false, err
		}

		if bytes != nil {
			return bytes, true, nil
		}
		return nil, false, nil
	*/

	val, found := proxy.cache.Get(string(key))

	if !found {
		rawBytes, succ, err := proxy.rawGet(key)
		if err == nil && succ {
			// log.Printf("found raw key %s", key)
			proxy.cache.Set(string(key), rawBytes, gocache.DefaultExpiration)
		} else if err == nil {
			// log.Printf("not found key %s", key)
			proxy.cache.Set(string(key), 0, gocache.DefaultExpiration)
		}

		return rawBytes, succ, err
	}

	if bytes, ok := val.([]byte); ok {
		// log.Printf("found key %s", key)
		return bytes, true, nil
	}

	return nil, false, nil

}

func (proxy *Proxy) rawGet(key []byte) ([]byte, bool, error) {
	var js interface{}
	var err error
	_, err = proxy.bucket.Get(string(key), &js)
	if err != nil {
		if gocb.IsKeyNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("Couchbase report error: %s", err)
	}
	bytes, err := json.Marshal(js)
	if err != nil {
		return nil, false, fmt.Errorf("Json marshal error: %s", err)
	}
	return bytes, true, nil
}

func ttl(expire uint32) uint32 {
	if expire == 0 {
		return 0
	}

	if expire <= (30 * 24 * 60 * 60) {
		return uint32(time.Now().Unix() + int64(expire))
	}

	return expire
}

func (proxy *Proxy) expire(key []byte, expire uint32) error {
	_, err := proxy.bucket.Touch(string(key), 0, ttl(expire))
	return err
}

func (proxy *Proxy) set(key []byte, value []byte, expire uint32) error {
	var js interface{}
	var err error
	err = json.Unmarshal(value, &js)
	if err != nil {
		log.Printf("set value failed because %s", err)
		return err
	}

	// log.Printf("set %s with ttl %d", key, ttl(expire))
	_, err = proxy.bucket.Upsert(string(key), js, ttl(expire))
	return err
}

func (proxy *Proxy) remove(key []byte) bool {
	_, err := proxy.bucket.Remove(string(key), 0)
	if err != nil {
		return false
	}
	return true
}

func (proxy *Proxy) onConnect(conn redcon.Conn) bool {
	return true
}

func (proxy *Proxy) onDisconnect(conn redcon.Conn, err error) {

}

func (proxy *Proxy) onCommand(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		log.Printf("Unknown command %s", cmd.Args[0])
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		conn.Close()
	case "set":
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		var expire int
		var err error
		if len(cmd.Args) > 3 {
			if strings.ToLower(string(cmd.Args[3])) == "ex" {
				expire, err = strconv.Atoi(string(cmd.Args[4]))
				if err != nil {
					conn.WriteError("ERR wrong argument for '" + string(cmd.Args[0]) + "' ex parameter")
					return
				}
			} else if strings.ToLower(string(cmd.Args[3])) == "px" {
				expire, err = strconv.Atoi(string(cmd.Args[4]))
				expire = expire / 1000
			} else {
				conn.WriteError(fmt.Sprintf("ERR unknown parameter for %s: %s", string(cmd.Args[0]), cmd.Args[3]))
			}
		}
		err = proxy.set(cmd.Args[1], cmd.Args[2], uint32(expire))
		if err != nil {
			conn.WriteError("ERR on set " + err.Error())
		} else {
			conn.WriteString("OK")
		}
	case "get":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		bytes, ok, err := proxy.get(cmd.Args[1])
		if err != nil {
			conn.WriteError(fmt.Sprintf("Couchbase report error: %s", err))
		} else if ok {
			conn.WriteBulk(bytes)
		} else {
			conn.WriteNull()
		}

	case "mget":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for get'" + string(cmd.Args[0]) + "' command")
			return
		}
		conn.WriteArray(len(cmd.Args) - 1)

		for i := 1; i < len(cmd.Args); i++ {
			bytes, ok, err := proxy.get(cmd.Args[i])
			if err != nil {
				conn.WriteError(fmt.Sprintf("Couchbase report error: %s", err))
			} else if ok {
				conn.WriteBulk(bytes)
			} else {
				conn.WriteNull()
			}
		}
	case "mset":
		if len(cmd.Args) < 3 || len(cmd.Args)%2 != 1 {
			conn.WriteError("ERR wrong number of arguments for mset")
			return
		}

		for i := 1; i < len(cmd.Args); i += 2 {
			proxy.set(cmd.Args[i], cmd.Args[i+1], 0)
		}
		conn.WriteString("OK")

	case "del":
		if len(cmd.Args) != 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		ok := proxy.remove(cmd.Args[1])
		if !ok {
			conn.WriteInt(0)
		} else {
			conn.WriteInt(1)
		}

	case "expire":
		if len(cmd.Args) != 3 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for %s", string(cmd.Args[0])))
			return
		}
		val, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR invalid expire format %s", string(cmd.Args[2])))
			return
		}
		proxy.expire(cmd.Args[1], uint32(val))
		conn.WriteString("OK")

	case "pexpire":
		if len(cmd.Args) != 3 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for %s", string(cmd.Args[0])))
			return
		}
		val, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR invalid expire format %s", string(cmd.Args[2])))
			return
		}
		proxy.expire(cmd.Args[1], uint32(val/1000))
		conn.WriteString("OK")

	case "ttl":
		if len(cmd.Args) != 3 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for %s", string(cmd.Args[0])))
			return
		}
		val, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR invalid expire format %s", string(cmd.Args[2])))
			return
		}
		proxy.expire(cmd.Args[1], uint32(val))
		conn.WriteString("OK")

	case "pttl":
		if len(cmd.Args) != 3 {
			conn.WriteError(fmt.Sprintf("ERR wrong number of arguments for %s", string(cmd.Args[0])))
			return
		}
		val, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR invalid expire format %s", string(cmd.Args[2])))
			return
		}
		proxy.expire(cmd.Args[1], uint32(val))
		conn.WriteString("OK")
	}

}

// Run proxy server
func (proxy *Proxy) Run() {

	log.Printf("start proxy on %s", proxy.listen)
	err := redcon.ListenAndServe(proxy.listen,
		proxy.onCommand,
		proxy.onConnect,
		proxy.onDisconnect)

	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	listen := flag.String("listen", ":6380", "listen ip and port")
	couchbase := flag.String("couchbase", "couchbase://localhost", "couchbase server")
	user := flag.String("user", "", "user name")
	password := flag.String("password", "", "password")
	bucket := flag.String("bucket", "", "bucket name")
	cacheTTL := flag.Int("cacheTTL", 300, "cache ttl time")
	useCache := flag.Bool("useCache", false, "use cache")

	flag.Parse()

	log.Printf("cacheTTL=%d", *cacheTTL)

	proxy, err := NewProxy(*listen, *couchbase, *user, *password, *bucket, *useCache, *cacheTTL)

	if err != nil {
		log.Fatal(err)
	}

	proxy.Run()
}

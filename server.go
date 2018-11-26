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
	}

	return &Proxy{bucket: bkt, listen: listen, cache: cache}, nil
}

func (proxy *Proxy) get(key []byte) ([]byte, bool, error) {
	if proxy.cache == nil {
		return proxy.rawGet(key)
	}

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

func (proxy *Proxy) mset(kvs [][]byte) error {
	if len(kvs)%2 != 0 {
		return fmt.Errorf("mset error: odd length")
	}

	if proxy.cache == nil {
		return proxy.rawMSet(kvs)
	}

	for i := 0; i < len(kvs)/2; i++ {
		key := kvs[i*2]
		val := kvs[i*2+1]
		proxy.cache.Set(string(key), val, gocache.DefaultExpiration)
	}

	return proxy.rawMSet(kvs)
}

func (proxy *Proxy) rawMSet(kvs [][]byte) error {
	if len(kvs)%2 != 0 {
		return fmt.Errorf("mset error: odd length")
	}

	upsertOps := make([]gocb.UpsertOp, len(kvs)/2)
	bulkOps := make([]gocb.BulkOp, len(kvs)/2)

	for i := 0; i < len(kvs)/2; i++ {
		var js interface{}
		var err error
		err = json.Unmarshal(kvs[i*2+1], &js)
		if err != nil {
			return fmt.Errorf("mset error: key %s unmarshal error: %s", string(kvs[i*2]), err)
		}

		upsertOps[i] = gocb.UpsertOp{Key: string(kvs[i*2]), Value: js}
		bulkOps[i] = &upsertOps[i]
	}

	err := proxy.bucket.Do(bulkOps)

	for i := range upsertOps {
		if upsertOps[i].Err != nil {
			log.Printf("mset error: key %s upsert error: %s", upsertOps[i].Key, upsertOps[i].Err)
		}
	}

	return err
}

func (proxy *Proxy) mget(keys [][]byte) ([][]byte, error) {
	if proxy.cache == nil {
		return proxy.rawMGet(keys)
	}

	result := make([][]byte, len(keys))
	var missed = []int{}
	var missedKeys = [][]byte{}
	for i := range keys {
		val, found := proxy.cache.Get(string(keys[i]))
		if !found {
			missed = append(missed, i)
			missedKeys = append(missedKeys, keys[i])
		} else {
			if bytes, ok := val.([]byte); ok {
				result[i] = bytes
			} else {
				result[i] = nil
			}
		}
	}
	if len(missed) > 0 {
		missedResult, err := proxy.rawMGet(missedKeys)
		if err != nil {
			log.Printf("mget error: %s", err)
			return nil, err
		}
		for k := range missed {
			if missedResult[k] == nil {
				proxy.cache.Set(string(missedKeys[k]), 0, gocache.DefaultExpiration)
			} else {
				proxy.cache.Set(string(missedKeys[k]), missedResult[k], gocache.DefaultExpiration)
			}
			result[missed[k]] = missedResult[k]
		}
	}
	return result, nil
}

func (proxy *Proxy) rawMGet(keys [][]byte) ([][]byte, error) {
	getOps := make([]gocb.GetOp, len(keys))
	bulkOps := make([]gocb.BulkOp, len(keys))
	for i := range keys {
		var val interface{}
		getOps[i] = gocb.GetOp{Key: string(keys[i]), Value: &val}
		bulkOps[i] = &getOps[i]
	}

	err := proxy.bucket.Do(bulkOps)
	if err != nil {
		log.Printf("rawMGet failed: %s", err)
		return nil, err
	}

	var result = make([][]byte, len(getOps))

	for i := range getOps {
		item := getOps[i]
		if item.Err != nil {
			if gocb.IsKeyNotFoundError(item.Err) {
				result[i] = nil
			} else {
				log.Printf("mget error: %s get item error: %s", item.Key, item.Err)
				result[i] = nil
			}
		} else {
			if item.Value == nil {
				result[i] = nil
			} else {
				bytes, err := json.Marshal(item.Value)
				if err != nil {
					log.Printf("mget error: unable to marshal key %s: %s", item.Key, item.Err)
					result[i] = nil
				}
				result[i] = bytes
			}
		}
	}

	return result, nil
}

func ttl(expire uint32) uint32 {
	if expire == 0 {
		return 0
	}

	// if expire <= (30 * 24 * 60 * 60) {
	//	return uint32(time.Now().Unix() + int64(expire))
	//}

	return uint32(time.Now().Unix() + int64(expire))
}

func (proxy *Proxy) expire(key []byte, expire uint32) error {
	_, err := proxy.bucket.Touch(string(key), 0, ttl(expire))
	return err
}

func (proxy *Proxy) ttl(key []byte, expire uint32) error {
	_, err := proxy.bucket.Touch(string(key), 0, expire)
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

	if err != nil {
		return err
	}

	if proxy.cache != nil {
		proxy.cache.Set(string(key), value, gocache.DefaultExpiration)
	}
	return nil
}

func (proxy *Proxy) remove(key []byte) bool {
	_, err := proxy.bucket.Remove(string(key), 0)
	if err != nil {
		return false
	}

	if proxy.cache != nil {
		proxy.cache.Delete(string(key))
	}
	return true
}

func (proxy *Proxy) rawMDel(keys [][]byte) (int, error) {
	delOps := make([]gocb.RemoveOp, len(keys))
	bulkOps := make([]gocb.BulkOp, len(keys))

	for i := 0; i < len(keys); i++ {
		delOps[i] = gocb.RemoveOp{Key: string(keys[i])}
		bulkOps[i] = &delOps[i]
	}

	err := proxy.bucket.Do(bulkOps)

	if err != nil {
		log.Printf("mdel error: %s", err)
		return 0, err
	}

	var ret int = 0

	for i := range delOps {
		if delOps[i].Err != nil {
			if gocb.IsKeyNotFoundError(delOps[i].Err) {
				// pass
			} else {
				log.Printf("mset error: key %s upsert error: %s", delOps[i].Key, delOps[i].Err)
			}
		} else {
			ret++
		}
	}

	return ret, nil
}

func (proxy *Proxy) mdel(keys [][]byte) (int, error) {

	ret, err := proxy.rawMDel(keys)
	if err != nil {
		return ret, err
	}

	if proxy.cache != nil {
		for i := range keys {
			proxy.cache.Delete(string(keys[i]))
		}
	}

	return ret, nil
}

func (proxy *Proxy) exists(keys [][]byte) (int, error) {
	result, err := proxy.mget(keys)
	if err != nil {
		return 0, err
	}
	var i = 0
	for _, bytes := range result {
		if bytes != nil {
			i = i + 1
		}
	}
	return i, nil
}

func (proxy *Proxy) onConnect(conn redcon.Conn) bool {
	log.Printf("client %s connected", conn.RemoteAddr())
	return true
}

func (proxy *Proxy) onDisconnect(conn redcon.Conn, err error) {
	log.Printf("client %s disconnected", conn.RemoteAddr())
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
		result, err := proxy.mget(cmd.Args[1:])
		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR mget failed: %s", err))
			return
		}

		conn.WriteArray(len(result))
		for _, bytes := range result {
			if bytes != nil {
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

		err := proxy.mset(cmd.Args[1:])

		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR mset failed: %s", err))
		} else {
			conn.WriteString("OK")
		}

	case "del":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		ret, err := proxy.mdel(cmd.Args[1:])
		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR del failed: %s", err))
			return
		}
		conn.WriteInt(ret)
		return

	case "exists":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}
		ret, err := proxy.exists(cmd.Args[1:])
		if err != nil {
			conn.WriteError(fmt.Sprintf("ERR exists failed: %s", err))
			return
		}
		conn.WriteInt(ret)
		return

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
		proxy.ttl(cmd.Args[1], uint32(val))
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
		proxy.ttl(cmd.Args[1], uint32(val/1000))
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

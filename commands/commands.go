package commands

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/skantade/kv-cache/resp"
	"github.com/skantade/kv-cache/store"
)

// Handler dispatches Redis commands to the store.
type Handler struct {
	store    *store.Store
	startAt  time.Time
	cmdCount atomic.Int64
	connCount atomic.Int32
}

func NewHandler(s *store.Store) *Handler {
	return &Handler{store: s, startAt: time.Now()}
}

func (h *Handler) IncrConn() { h.connCount.Add(1) }
func (h *Handler) DecrConn() { h.connCount.Add(-1) }

// Handle dispatches a RESP command. args[0] is the command name, args[1:] are arguments.
func (h *Handler) Handle(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR empty command")
	}
	h.cmdCount.Add(1)
	cmd := strings.ToUpper(args[0].Str)

	switch cmd {
	// Connection
	case "PING":
		return h.cmdPing(args[1:])
	case "ECHO":
		return h.cmdEcho(args[1:])
	case "QUIT":
		return resp.SimpleString("OK")
	case "COMMAND":
		return resp.SimpleString("OK")
	case "SELECT":
		return h.cmdSelect(args[1:])

	// String
	case "SET":
		return h.cmdSet(args[1:])
	case "GET":
		return h.cmdGet(args[1:])
	case "GETDEL":
		return h.cmdGetDel(args[1:])
	case "GETSET":
		return h.cmdGetSet(args[1:])
	case "GETEX":
		return h.cmdGetEx(args[1:])
	case "MSET":
		return h.cmdMSet(args[1:])
	case "MSETNX":
		return h.cmdMSetNX(args[1:])
	case "MGET":
		return h.cmdMGet(args[1:])
	case "APPEND":
		return h.cmdAppend(args[1:])
	case "STRLEN":
		return h.cmdStrLen(args[1:])
	case "INCR":
		return h.cmdIncr(args[1:])
	case "INCRBY":
		return h.cmdIncrBy(args[1:])
	case "INCRBYFLOAT":
		return h.cmdIncrByFloat(args[1:])
	case "DECR":
		return h.cmdDecr(args[1:])
	case "DECRBY":
		return h.cmdDecrBy(args[1:])
	case "SETNX":
		return h.cmdSetNX(args[1:])
	case "SETEX":
		return h.cmdSetEx(args[1:])
	case "PSETEX":
		return h.cmdPSetEx(args[1:])

	// Key
	case "DEL":
		return h.cmdDel(args[1:])
	case "UNLINK":
		return h.cmdDel(args[1:]) // sync unlink
	case "EXISTS":
		return h.cmdExists(args[1:])
	case "EXPIRE":
		return h.cmdExpire(args[1:])
	case "PEXPIRE":
		return h.cmdPExpire(args[1:])
	case "EXPIREAT":
		return h.cmdExpireAt(args[1:])
	case "PEXPIREAT":
		return h.cmdPExpireAt(args[1:])
	case "TTL":
		return h.cmdTTL(args[1:])
	case "PTTL":
		return h.cmdPTTL(args[1:])
	case "PERSIST":
		return h.cmdPersist(args[1:])
	case "KEYS":
		return h.cmdKeys(args[1:])
	case "RANDOMKEY":
		return h.cmdRandomKey()
	case "RENAME":
		return h.cmdRename(args[1:])
	case "RENAMENX":
		return h.cmdRenameNX(args[1:])
	case "TYPE":
		return h.cmdType(args[1:])
	case "OBJECT":
		return h.cmdObject(args[1:])

	// Server
	case "DBSIZE":
		return resp.Integer(h.store.DBSize())
	case "FLUSHALL", "FLUSHDB":
		h.store.FlushAll()
		return resp.SimpleString("OK")
	case "INFO":
		return h.cmdInfo(args[1:])
	case "DEBUG":
		return h.cmdDebug(args[1:])
	case "CONFIG":
		return h.cmdConfig(args[1:])
	case "LATENCY":
		return h.cmdLatency(args[1:])
	case "WAIT":
		return resp.Integer(0)
	case "CLIENT":
		return h.cmdClient(args[1:])
	case "SAVE", "BGSAVE":
		return resp.SimpleString("OK")
	case "BGREWRITEAOF":
		return resp.SimpleString("OK")
	case "LASTSAVE":
		return resp.Integer(time.Now().Unix())
	case "SLOWLOG":
		return h.cmdSlowlog(args[1:])
	case "SWAPDB":
		return resp.Err("ERR SWAPDB not supported")

	default:
		return resp.Err(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}
}

// ---------------------------------------------------------------------------
// Connection commands
// ---------------------------------------------------------------------------

func (h *Handler) cmdPing(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.SimpleString("PONG")
	}
	return resp.BulkString(args[0].Str)
}

func (h *Handler) cmdEcho(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'echo' command")
	}
	return resp.BulkString(args[0].Str)
}

func (h *Handler) cmdSelect(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'select' command")
	}
	n, err := strconv.ParseInt(args[0].Str, 10, 64)
	if err != nil || n != 0 {
		return resp.Err("ERR DB index is out of range")
	}
	return resp.SimpleString("OK")
}

// ---------------------------------------------------------------------------
// String commands
// ---------------------------------------------------------------------------

func (h *Handler) cmdSet(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.Err("ERR wrong number of arguments for 'set' command")
	}
	key := args[0].Str
	value := args[1].Str

	var ttl time.Duration
	var nx, xx, get bool

	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Str)
		switch opt {
		case "EX":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			i++
			secs, err := strconv.ParseInt(args[i].Str, 10, 64)
			if err != nil || secs <= 0 {
				return resp.Err("ERR invalid expire time in 'set' command")
			}
			ttl = time.Duration(secs) * time.Second
		case "PX":
			if i+1 >= len(args) {
				return resp.Err("ERR syntax error")
			}
			i++
			ms, err := strconv.ParseInt(args[i].Str, 10, 64)
			if err != nil || ms <= 0 {
				return resp.Err("ERR invalid expire time in 'set' command")
			}
			ttl = time.Duration(ms) * time.Millisecond
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "KEEPTTL":
			// handled below
		}
	}

	if nx && xx {
		return resp.Err("ERR XX and NX options at the same time are not compatible")
	}

	var old string
	var hadOld bool
	if get {
		old, hadOld = h.store.Get(key)
	}

	if nx {
		if !h.store.SetNX(key, value, ttl) {
			if get {
				if hadOld {
					return resp.BulkString(old)
				}
				return resp.NullBulkString()
			}
			return resp.NullBulkString()
		}
	} else if xx {
		if !h.store.SetXX(key, value, ttl) {
			if get {
				if hadOld {
					return resp.BulkString(old)
				}
				return resp.NullBulkString()
			}
			return resp.NullBulkString()
		}
	} else {
		h.store.Set(key, value, ttl)
	}

	if get {
		if hadOld {
			return resp.BulkString(old)
		}
		return resp.NullBulkString()
	}
	return resp.SimpleString("OK")
}

func (h *Handler) cmdGet(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'get' command")
	}
	val, ok := h.store.Get(args[0].Str)
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(val)
}

func (h *Handler) cmdGetDel(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'getdel' command")
	}
	val, ok := h.store.GetDel(args[0].Str)
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(val)
}

func (h *Handler) cmdGetSet(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'getset' command")
	}
	old, had := h.store.GetSet(args[0].Str, args[1].Str)
	if !had {
		return resp.NullBulkString()
	}
	return resp.BulkString(old)
}

func (h *Handler) cmdGetEx(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.Err("ERR wrong number of arguments for 'getex' command")
	}
	key := args[0].Str
	val, ok := h.store.Get(key)
	if !ok {
		return resp.NullBulkString()
	}
	if len(args) > 1 {
		opt := strings.ToUpper(args[1].Str)
		switch opt {
		case "EX":
			if len(args) < 3 {
				return resp.Err("ERR syntax error")
			}
			secs, err := strconv.ParseInt(args[2].Str, 10, 64)
			if err != nil || secs <= 0 {
				return resp.Err("ERR invalid expire time in 'getex' command")
			}
			h.store.Expire(key, time.Duration(secs)*time.Second)
		case "PX":
			if len(args) < 3 {
				return resp.Err("ERR syntax error")
			}
			ms, err := strconv.ParseInt(args[2].Str, 10, 64)
			if err != nil || ms <= 0 {
				return resp.Err("ERR invalid expire time in 'getex' command")
			}
			h.store.Expire(key, time.Duration(ms)*time.Millisecond)
		case "PERSIST":
			h.store.Persist(key)
		}
	}
	return resp.BulkString(val)
}

func (h *Handler) cmdMSet(args []resp.Value) resp.Value {
	if len(args) == 0 || len(args)%2 != 0 {
		return resp.Err("ERR wrong number of arguments for 'mset' command")
	}
	pairs := make(map[string]string, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		pairs[args[i].Str] = args[i+1].Str
	}
	h.store.MSet(pairs)
	return resp.SimpleString("OK")
}

func (h *Handler) cmdMSetNX(args []resp.Value) resp.Value {
	if len(args) == 0 || len(args)%2 != 0 {
		return resp.Err("ERR wrong number of arguments for 'msetnx' command")
	}
	pairs := make(map[string]string, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		pairs[args[i].Str] = args[i+1].Str
	}
	// Check all keys don't exist
	for k := range pairs {
		if h.store.Exists(k) > 0 {
			return resp.Integer(0)
		}
	}
	h.store.MSet(pairs)
	return resp.Integer(1)
}

func (h *Handler) cmdMGet(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR wrong number of arguments for 'mget' command")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.Str
	}
	results := h.store.MGet(keys)
	vals := make([]resp.Value, len(results))
	for i, r := range results {
		if r == nil {
			vals[i] = resp.NullBulkString()
		} else {
			vals[i] = resp.BulkString(r.(string))
		}
	}
	return resp.Array(vals...)
}

func (h *Handler) cmdAppend(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'append' command")
	}
	n := h.store.Append(args[0].Str, args[1].Str)
	return resp.Integer(n)
}

func (h *Handler) cmdStrLen(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'strlen' command")
	}
	return resp.Integer(h.store.StrLen(args[0].Str))
}

func (h *Handler) cmdIncr(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'incr' command")
	}
	n, err := h.store.Incr(args[0].Str)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Integer(n)
}

func (h *Handler) cmdIncrBy(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'incrby' command")
	}
	delta, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	n, err := h.store.IncrBy(args[0].Str, delta)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Integer(n)
}

func (h *Handler) cmdIncrByFloat(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'incrbyfloat' command")
	}
	delta, err := strconv.ParseFloat(args[1].Str, 64)
	if err != nil {
		return resp.Err("ERR value is not a valid float")
	}
	key := args[0].Str
	cur := 0.0
	if val, ok := h.store.Get(key); ok {
		cur, err = strconv.ParseFloat(val, 64)
		if err != nil {
			return resp.Err("ERR value is not a valid float")
		}
	}
	result := cur + delta
	if math.IsInf(result, 0) || math.IsNaN(result) {
		return resp.Err("ERR increment would produce NaN or Infinity")
	}
	str := strconv.FormatFloat(result, 'f', -1, 64)
	h.store.Set(key, str, 0)
	return resp.BulkString(str)
}

func (h *Handler) cmdDecr(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'decr' command")
	}
	n, err := h.store.Decr(args[0].Str)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Integer(n)
}

func (h *Handler) cmdDecrBy(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'decrby' command")
	}
	delta, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	n, err := h.store.DecrBy(args[0].Str, delta)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Integer(n)
}

func (h *Handler) cmdSetNX(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'setnx' command")
	}
	if h.store.SetNX(args[0].Str, args[1].Str, 0) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (h *Handler) cmdSetEx(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.Err("ERR wrong number of arguments for 'setex' command")
	}
	secs, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil || secs <= 0 {
		return resp.Err("ERR invalid expire time in 'setex' command")
	}
	h.store.Set(args[0].Str, args[2].Str, time.Duration(secs)*time.Second)
	return resp.SimpleString("OK")
}

func (h *Handler) cmdPSetEx(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.Err("ERR wrong number of arguments for 'psetex' command")
	}
	ms, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil || ms <= 0 {
		return resp.Err("ERR invalid expire time in 'psetex' command")
	}
	h.store.Set(args[0].Str, args[2].Str, time.Duration(ms)*time.Millisecond)
	return resp.SimpleString("OK")
}

// ---------------------------------------------------------------------------
// Key commands
// ---------------------------------------------------------------------------

func (h *Handler) cmdDel(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR wrong number of arguments for 'del' command")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.Str
	}
	return resp.Integer(h.store.Del(keys...))
}

func (h *Handler) cmdExists(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR wrong number of arguments for 'exists' command")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.Str
	}
	return resp.Integer(h.store.Exists(keys...))
}

func (h *Handler) cmdExpire(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'expire' command")
	}
	secs, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	if h.store.Expire(args[0].Str, time.Duration(secs)*time.Second) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (h *Handler) cmdPExpire(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'pexpire' command")
	}
	ms, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	if h.store.Expire(args[0].Str, time.Duration(ms)*time.Millisecond) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (h *Handler) cmdExpireAt(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'expireat' command")
	}
	ts, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	t := time.Unix(ts, 0)
	if h.store.ExpireAt(args[0].Str, t) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (h *Handler) cmdPExpireAt(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'pexpireat' command")
	}
	ms, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.Err("ERR value is not an integer or out of range")
	}
	t := time.UnixMilli(ms)
	if h.store.ExpireAt(args[0].Str, t) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (h *Handler) cmdTTL(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'ttl' command")
	}
	d := h.store.TTL(args[0].Str)
	if d == -2*time.Second {
		return resp.Integer(-2)
	}
	if d == -1*time.Second {
		return resp.Integer(-1)
	}
	return resp.Integer(int64(d / time.Second))
}

func (h *Handler) cmdPTTL(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'pttl' command")
	}
	d := h.store.PTTL(args[0].Str)
	if d == -2*time.Millisecond {
		return resp.Integer(-2)
	}
	if d == -1*time.Millisecond {
		return resp.Integer(-1)
	}
	return resp.Integer(int64(d / time.Millisecond))
}

func (h *Handler) cmdPersist(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'persist' command")
	}
	if h.store.Persist(args[0].Str) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (h *Handler) cmdKeys(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'keys' command")
	}
	keys := h.store.Keys(args[0].Str)
	vals := make([]resp.Value, len(keys))
	for i, k := range keys {
		vals[i] = resp.BulkString(k)
	}
	return resp.Array(vals...)
}

func (h *Handler) cmdRandomKey() resp.Value {
	key, ok := h.store.RandomKey()
	if !ok {
		return resp.NullBulkString()
	}
	return resp.BulkString(key)
}

func (h *Handler) cmdRename(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'rename' command")
	}
	if err := h.store.Rename(args[0].Str, args[1].Str); err != nil {
		return resp.Err(err.Error())
	}
	return resp.SimpleString("OK")
}

func (h *Handler) cmdRenameNX(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.Err("ERR wrong number of arguments for 'renamenx' command")
	}
	if h.store.RenameNX(args[0].Str, args[1].Str) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func (h *Handler) cmdType(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.Err("ERR wrong number of arguments for 'type' command")
	}
	return resp.SimpleString(h.store.Type(args[0].Str))
}

func (h *Handler) cmdObject(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR wrong number of arguments for 'object' command")
	}
	sub := strings.ToUpper(args[0].Str)
	switch sub {
	case "ENCODING":
		if len(args) < 2 {
			return resp.Err("ERR wrong number of arguments for 'object|encoding' command")
		}
		if h.store.Type(args[1].Str) == "none" {
			return resp.Err("ERR no such key")
		}
		return resp.BulkString("embstr")
	case "REFCOUNT":
		if len(args) < 2 {
			return resp.Err("ERR wrong number of arguments for 'object|refcount' command")
		}
		if h.store.Type(args[1].Str) == "none" {
			return resp.Err("ERR no such key")
		}
		return resp.Integer(1)
	case "IDLETIME":
		if len(args) < 2 {
			return resp.Err("ERR wrong number of arguments for 'object|idletime' command")
		}
		if h.store.Type(args[1].Str) == "none" {
			return resp.Err("ERR no such key")
		}
		return resp.Integer(0)
	case "HELP":
		return resp.Array(
			resp.BulkString("OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
			resp.BulkString("ENCODING <key> -- Return the kind of internal representation the Redis object stored at <key> is using."),
			resp.BulkString("REFCOUNT <key> -- Return the reference count of the object stored at <key>."),
			resp.BulkString("IDLETIME <key> -- Return the idle time of the object stored at <key>."),
		)
	default:
		return resp.Err(fmt.Sprintf("ERR unknown subcommand '%s' for 'object'", sub))
	}
}

// ---------------------------------------------------------------------------
// Server commands
// ---------------------------------------------------------------------------

func (h *Handler) cmdInfo(args []resp.Value) resp.Value {
	uptime := int64(time.Since(h.startAt).Seconds())
	dbsize := h.store.DBSize()
	info := fmt.Sprintf(
		"# Server\r\nredis_version:7.0.0\r\ntcp_port:6380\r\nuptime_in_seconds:%d\r\n\r\n"+
			"# Clients\r\nconnected_clients:%d\r\n\r\n"+
			"# Memory\r\nused_memory:0\r\nused_memory_human:0B\r\n\r\n"+
			"# Stats\r\ntotal_commands_processed:%d\r\n\r\n"+
			"# Keyspace\r\ndb0:keys=%d,expires=0,avg_ttl=0\r\n",
		uptime,
		h.connCount.Load(),
		h.cmdCount.Load(),
		dbsize,
	)
	return resp.BulkString(info)
}

func (h *Handler) cmdDebug(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR wrong number of arguments for 'debug' command")
	}
	sub := strings.ToUpper(args[0].Str)
	switch sub {
	case "SLEEP":
		if len(args) < 2 {
			return resp.Err("ERR wrong number of arguments for 'debug|sleep' command")
		}
		secs, err := strconv.ParseFloat(args[1].Str, 64)
		if err != nil {
			return resp.Err("ERR value is not a float")
		}
		time.Sleep(time.Duration(secs * float64(time.Second)))
		return resp.SimpleString("OK")
	case "SET-ACTIVE-EXPIRE":
		return resp.SimpleString("OK")
	default:
		return resp.SimpleString("OK")
	}
}

func (h *Handler) cmdConfig(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR wrong number of arguments for 'config' command")
	}
	sub := strings.ToUpper(args[0].Str)
	switch sub {
	case "GET":
		if len(args) < 2 {
			return resp.Err("ERR wrong number of arguments for 'config|get' command")
		}
		param := strings.ToLower(args[1].Str)
		switch param {
		case "maxmemory":
			return resp.Array(resp.BulkString("maxmemory"), resp.BulkString("0"))
		case "hz":
			return resp.Array(resp.BulkString("hz"), resp.BulkString("10"))
		case "save":
			return resp.Array(resp.BulkString("save"), resp.BulkString(""))
		case "appendonly":
			return resp.Array(resp.BulkString("appendonly"), resp.BulkString("no"))
		default:
			return resp.Array()
		}
	case "SET":
		return resp.SimpleString("OK")
	case "RESETSTAT":
		return resp.SimpleString("OK")
	default:
		return resp.Err(fmt.Sprintf("ERR unknown subcommand '%s' for 'config'", sub))
	}
}

func (h *Handler) cmdLatency(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.SimpleString("OK")
	}
	sub := strings.ToUpper(args[0].Str)
	switch sub {
	case "RESET":
		return resp.SimpleString("OK")
	case "HISTORY", "LATEST":
		return resp.Array()
	default:
		return resp.Array()
	}
}

func (h *Handler) cmdClient(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR wrong number of arguments for 'client' command")
	}
	sub := strings.ToUpper(args[0].Str)
	switch sub {
	case "GETNAME":
		return resp.NullBulkString()
	case "SETNAME":
		return resp.SimpleString("OK")
	case "LIST":
		return resp.BulkString("id=0 addr=unknown\n")
	case "ID":
		return resp.Integer(0)
	case "INFO":
		return resp.BulkString("id=0\n")
	default:
		return resp.Err(fmt.Sprintf("ERR unknown subcommand '%s' for 'client'", sub))
	}
}

func (h *Handler) cmdSlowlog(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Array()
	}
	sub := strings.ToUpper(args[0].Str)
	switch sub {
	case "GET":
		return resp.Array()
	case "LEN":
		return resp.Integer(0)
	case "RESET":
		return resp.SimpleString("OK")
	default:
		return resp.Array()
	}
}

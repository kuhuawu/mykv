package main

import (
	"github.com/tidwall/redcon"
	kv "kv2"
	"kv2/db"
	"log"
	"strings"
	"sync"
	"time"
)

var ps redcon.PubSub

type Hash struct {
	data  map[string]map[string]string
	mutex sync.RWMutex
}

type node struct {
	Value    interface{}
	PrevNode *node
	NextNode *node
	Created  time.Time
}

// List 是一个简单的列表结构
type List struct {
	data  map[string][]string
	mutex sync.RWMutex
}

// hash
func (db *Hash) HSET(key string, field string, value string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.data[key][field] = value

}

func (db *Hash) HGET(key string, field string) string {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	value, ok := db.data[key][field]
	if ok != false {

	}
	return value
}
func (db *Hash) HKEYS(key string) []string {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	fields := make([]string, 0, 100)
	for field, _ := range db.data[key] {
		fields = append(fields, field)

	}
	return fields
}
func (db *Hash) HDEL(key string, fields []string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	for _, field := range fields {
		delete(db.data[key], field)
	}

}

//list

// LPUSH 向列表左侧添加一个或多个元素
func (db *List) LPUSH(key string, values []string) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if _, ok := db.data[key]; !ok {
		db.data[key] = []string{}
	}
	db.data[key] = append(values, db.data[key]...)
}

// LPOP 从列表左侧移除并返回第一个元素
func (db *List) LPOP(key string) (string, bool) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if _, ok := db.data[key]; !ok || len(db.data[key]) == 0 {
		return "", false
	}
	element := db.data[key][0]
	db.data[key] = db.data[key][1:]
	return element, true
}

func main() {

	// 初始化存储引擎

	// 设置数据库的目录和段大小
	dir := "C:/mykv"                        // 数据库存储目录
	segmentSize := int64(1024 * 1024 * 200) //  数据文件阈值大小200m

	// 创建 Options 结构体
	options := &kv.Options{
		Dir:         dir,
		SegmentSize: segmentSize,
	}

	// 初始化 DB 实例
	db, err := db.NewDB(options)
	if err != nil {
		log.Fatalf("Failed to initialize DB: %v", err)
	}
	err1 := db.Recovery(options)
	if err1 != nil {
		log.Printf("Failed to recover DB: %v", err1)
	}

	server := redcon.ListenAndServe(":8080", func(conn redcon.Conn, cmd redcon.Command) {

		switch strings.ToLower(string(cmd.Args[0])) {
		default:
			conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
		case "ping":
			conn.WriteString("PONG123")
		case "quit":
			conn.WriteString("OK")

			conn.Close()
		case "set":
			if len(cmd.Args) != 3 {
				conn.WriteError("ERR123 wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			key := cmd.Args[1]
			value := cmd.Args[2]

			db.Set(key, value)

			conn.WriteString("OK")
		case "get":
			if len(cmd.Args) != 2 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			val, ok := db.Get(cmd.Args[1])

			if ok != nil {
				conn.WriteNull()
			} else {
				conn.WriteBulk(val)
			}
		case "del":
			if len(cmd.Args) != 2 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}

			ok := db.Delete(cmd.Args[1])

			if ok != nil {
				conn.WriteInt(0)
			} else {
				conn.WriteInt(1)
			}
		case "publish":
			if len(cmd.Args) != 3 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
		case "subscribe", "psubscribe":
			if len(cmd.Args) < 2 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			command := strings.ToLower(string(cmd.Args[0]))
			for i := 1; i < len(cmd.Args); i++ {
				if command == "psubscribe" {
					ps.Psubscribe(conn, string(cmd.Args[i]))
				} else {
					ps.Subscribe(conn, string(cmd.Args[i]))
				}
			}
		}
	}, func(conn redcon.Conn) bool {
		// Use this function to accept or deny the connection.
		// log.Printf("accept: %s", conn.RemoteAddr())
		return true
	},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		})

	if server == nil {
		panic(server)
	}

}

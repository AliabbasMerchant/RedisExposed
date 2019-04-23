package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/go-redis/redis"
)

var client *redis.Client

func getKey(s string) string {
	p := strings.Split(s, "/")
	return p[len(p)-1]
}
func setHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		for k := range r.URL.Query() {
			err := client.Set(key, k, 0).Err()
			if err != nil {
				panic(err)
			} else {
				w.Write([]byte("SET " + key + "=" + k))
			}
			break
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}
func getHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		val, err := client.Get(key).Result()
		if err == redis.Nil {
			w.Write([]byte(key + " does not exist"))
		} else if err != nil {
			panic(err)
		} else {
			w.Write([]byte(val))
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}
func delHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		err := client.Del(key).Err()
		if err != nil {
			panic(err)
		} else {
			w.Write([]byte("Deleted"))
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}
func existsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		val, err := client.Exists(key).Result()
		if err != nil {
			panic(err)
		} else {
			if val == 1 {
				w.Write([]byte("Exists"))
			} else {
				w.Write([]byte("Does not exist"))
			}
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}
func hmsetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		w.Write([]byte(key + " SET\n"))
		for k, v := range r.URL.Query() {
			m := make(map[string]interface{})
			m[k] = v[0]
			err := client.HMSet(key, m).Err()
			if err != nil {
				panic(err)
			} else {
				w.Write([]byte(k + "=" + v[0] + "\n"))
			}
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}
func hdelHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		for k := range r.URL.Query() {
			err := client.HDel(key, k).Err()
			if err != nil {
				panic(err)
			} else {
				w.Write([]byte(k + " Deleted"))
			}
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}
func hexistsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		for k := range r.URL.Query() {
			val, err := client.HExists(key, k).Result()
			if err != nil {
				panic(err)
			} else if val {
				w.Write([]byte(k + " Exists"))
			} else {
				w.Write([]byte(k + " Does not exist"))
			}
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}
func hgetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		for k := range r.URL.Query() {
			val, err := client.HGet(key, k).Result()
			if err != nil {
				w.Write([]byte("nil"))
			} else {
				w.Write([]byte(val))
			}
			break
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}
func hgetallHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		val, err := client.HGetAll(key).Result()
		if err == nil {
			for _, v := range val {
				w.Write([]byte(v + "\n"))
			}
		} else {
			panic(err)
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}
func hkeysHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		key := getKey(r.URL.Path)
		val, err := client.HKeys(key).Result()
		if err == nil {
			for _, v := range val {
				w.Write([]byte(v))
			}
		} else {
			panic(err)
		}
	} else {
		w.Write([]byte("Sorry, only GET methods are supported."))
	}
}

func main() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	http.HandleFunc("/set/", setHandler)
	http.HandleFunc("/get/", getHandler)
	http.HandleFunc("/del/", delHandler)
	http.HandleFunc("/exists/", existsHandler)
	http.HandleFunc("/hmset/", hmsetHandler)
	http.HandleFunc("/hdel/", hdelHandler)
	http.HandleFunc("/hexists/", hexistsHandler)
	http.HandleFunc("/hget/", hgetHandler)
	http.HandleFunc("/hgetall/", hgetallHandler)
	http.HandleFunc("/hkeys/", hkeysHandler)
	fmt.Printf("Starting server...\n")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

package main

import (
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/gomodule/redigo/redis"
	"log"
	"time"
)

type dbCreator struct {
	pool                   *redis.Pool
	c                      *redisearch.Client
	syntheticsCardinality  uint64
	syntheticsNumberFields uint64
	isSynthetics           bool
	syntheticsType         string
}

func (d *dbCreator) Init() {

	d.pool = &redis.Pool{
		MaxIdle: 5,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host, redis.DialConnectTimeout(1*time.Second),
				redis.DialReadTimeout(3000*time.Millisecond),
				redis.DialWriteTimeout(3000*time.Millisecond))
			if err != nil {
				return nil, err
			}
			return c, err
		},
		MaxActive: 100,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {

			_, err := c.Do("PING")
			if err != nil {
				log.Printf("[ERROR]: TestOnBorrow failed healthcheck to redisUrl=%q err=%v",
					host, err)
			}
			return err
		},
		Wait: true, // pool.Get() will block waiting for a free connection
	}

}

func (d *dbCreator) DBExists(dbName string) (result bool) {
	if d.isSynthetics {
		conn := d.pool.Get()
		defer conn.Close()
		result = true
		_, err := conn.Do("FT.INFO", dbName)
		if err != nil {
			result = false
		}
	}
	return
}

func (d *dbCreator) RemoveOldDB(dbName string) (err error) {
	if d.isSynthetics {
		conn := d.pool.Get()
		defer conn.Close()
		_, err = conn.Do("FT.DROP", dbName)
	}
	return
}

func (d *dbCreator) CreateDB(dbName string) (err error) {
	if d.isSynthetics {
		conn := d.pool.Get()
		defer conn.Close()
		d.c = redisearch.NewClient(host, dbName)
		sc := redisearch.NewSchema(redisearch.DefaultOptions)

		// Create a schema
		for i := 0; uint64(i) < d.syntheticsNumberFields; i++ {
			sc.AddField(redisearch.NewNumericFieldOptions(fmt.Sprintf("field_%d", i+1), redisearch.NumericFieldOptions{Sortable: true}))
		}
		err = d.c.CreateIndex(sc)
	}
	return
}

func (d *dbCreator) Close() {
	d.pool.Close()
}

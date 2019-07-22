package main

import (
	"github.com/gomodule/redigo/redis"
	"log"
	"time"
)

type dbCreator struct {
	pool *redis.Pool
}

func (d *dbCreator) Init() {
	//d.pool.writeBytes([]byte(""))

	d.pool = &redis.Pool{
		MaxIdle:     5,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host, redis.DialConnectTimeout(1*time.Second),
				redis.DialReadTimeout(100*time.Millisecond),
				redis.DialWriteTimeout(100*time.Millisecond),)
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

func (d *dbCreator) DBExists(dbName string) bool {
	return true
}

// Isn't supported with interleaved groups?
func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	return nil
}

func (d *dbCreator) Close() {
	d.pool.Close()
}

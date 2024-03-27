package redis

import (
	"time"

	gredis "github.com/go-redis/redis/v7"
)

const defaultRecordTtl = 5 * time.Minute

type Cache struct {
	client *gredis.Client
}

func NewCache() *Cache {
	opts := &gredis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}

	client := gredis.NewClient(opts)

	return &Cache{
		client: client,
	}
}

func (c *Cache) Get(key string) (string, error) {
	return c.client.Get(key).Result()
}

func (c *Cache) Set(key, value string) error {
	return c.client.Set(key, value, defaultRecordTtl).Err()
}

func (c *Cache) Delete(key ...string) error {
	return c.client.Del(key...).Err()
}

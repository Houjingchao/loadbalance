package loadbalance

import (
	"github.com/Houjingchao/loadbalance/internal"
)

type LoadBalance struct {
	buckets []string
	hashCli *internal.Consistent
}

func NewLoadBalance(buckets []string) *LoadBalance {
	c := internal.New()
	for _, bucket := range buckets {
		c.Add(bucket)
	}
	return &LoadBalance{buckets: buckets, hashCli: c}
}

func (lb *LoadBalance) Get(key string) (string, error) {
	c := lb.hashCli
	b, err := c.Get(key)
	if err != nil {
		return "", err
	}
	return b, nil
}

func (lb *LoadBalance) Update(b []string) {
	lb.buckets = b
	lb.hashCli.Set(b)
	return
}

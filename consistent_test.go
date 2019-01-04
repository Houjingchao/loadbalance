package loadbalance

import (
	"crypto/rand"
	"math"
	"strings"
	"testing"
)

func TestLoadBalance_GetBucket(t *testing.T) {
	buckets := NewLoadBalance(strings.Split("c1,c2", ","))
	res, _ := buckets.Get("1234sadf56")
	t.Logf("res:%#v\n", res)
}

func TestLoadBalance_GetConsistent(t *testing.T) {
	buckets := genBuckets(5)
	hashClient := NewLoadBalance(buckets)
	for k := 0; k < 100; k++ {
		targets := make(map[string]struct{}, 0)
		key := genKey()
		for i := 0; i < 10000; i++ {
			bucket, err := hashClient.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			targets[bucket] = struct{}{}
		}

		if len(targets) != 1 {
			t.Fatal("hash not consistent")
		}
	}
}

func TestLoadBalance_GetBalance(t *testing.T) {
	buckets := genBuckets(2)
	hashClient := NewLoadBalance(buckets)
	counts := make(map[string]int, 0)
	keyNumber := 1000000

	for i := 0; i < keyNumber; i++ {
		key := genKey()
		bucket, err := hashClient.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		count, _ := counts[bucket]
		counts[bucket] = count + 1
	}
	countList := make([]float64, 0)
	for _, v := range counts {
		countList = append(countList, float64(v))
	}

	_, avg, _, std := statistic(countList)
	t.Logf("counts:%#v, avg:%#v, standard: %#v\n", countList, avg, std)
	if std > avg {
		t.Fatal("variance is too big")
	}
}

func TestLoadBalance_UpdateAdd(t *testing.T) {
	keyNumber := 10000
	bucketNumber := 10
	buckets1 := genBuckets(bucketNumber)
	buckets2 := append(buckets1, genKey())
	offsetNumber := 0
	for i := 0; i < keyNumber; i++ {
		key := genKey()
		hashClient := NewLoadBalance(buckets1)
		bucket1, _ := hashClient.Get(key)
		hashClient.Update(buckets2)
		bucket2, _ := hashClient.Get(key)
		if bucket1 != bucket2 {
			offsetNumber += 1
		}
	}
	offsetRate := float64(offsetNumber) / float64(keyNumber)
	t.Logf("ofsset rate:%#v bucket rate:%#v\n", offsetRate, 1/float64(bucketNumber+1))
	offset := math.Abs(offsetRate*float64(bucketNumber+1) - 1)
	t.Logf("offset:%#v\n", offset)
	if offset > 0.1*float64(bucketNumber) {
		t.Fatal("over offset")
	}
}

func BenchmarkLoadBalance_Get(b *testing.B) {
	buckets := genBuckets(50)
	hashClient := NewLoadBalance(buckets)
	key := genKey()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := hashClient.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLoadBalance_GetParallel(b *testing.B) {
	buckets := genBuckets(50)
	hashClient := NewLoadBalance(buckets)
	key := genKey()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := hashClient.Get(key)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestLoadBalance_Coverage(t *testing.T) {
	buckets := genBuckets(5)
	hashClient := NewLoadBalance(buckets)
	t.Logf("[newclient] buckets: %#v members:%#v\n", hashClient.buckets, hashClient.hashCli.Members())
}

func genBuckets(n int) []string {
	var buckets []string
	buckets = []string{}
	bucketsMap := make(map[string]struct{}, 0)
	for {
		bucket := generateRandomString(32)
		if _, ok := bucketsMap[bucket]; ok {
			continue
		}
		buckets = append(buckets, bucket)
		bucketsMap[bucket] = struct{}{}

		if len(buckets) >= n {
			break
		}
	}
	return buckets
}

func genKey() string {
	return generateRandomString(32)
}

func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func generateRandomString(n int) string {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	bytes, err := generateRandomBytes(n)
	if err != nil {
		return ""
	}
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes)
}

func statistic(list []float64) (float64, float64, float64, float64) {
	var sum float64 = 0
	var varianceSum float64 = 0
	num := float64(len(list))
	for _, v := range list {
		sum += v
	}
	avg := sum / num
	for _, v := range list {
		varianceSum += math.Pow(v-avg, 2)
	}
	variance := varianceSum / num
	return sum, avg, variance, math.Pow(variance, 0.5)
}

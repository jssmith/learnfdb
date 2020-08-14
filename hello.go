package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func setKeys(db fdb.Database, n, nKeys, size int) error {
	start := time.Now()
	buf := make([]byte, size)
	for i := 0; i < n; i++ {
		rand.Read(buf)
		mykey := fmt.Sprintf("hello-%d", rand.Intn(nKeys))
		_, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
			tr.Set(fdb.Key(mykey), buf)
			return
		})
		if err != nil {
			return errors.Wrap(err, "Unable to set FDB database")
		}
	}
	elapsed := time.Now().Sub(start)
	rate := float64(n) / elapsed.Seconds()
	log.Printf("set completed %d iterations with rate %f", n, rate)
	return nil
}

func getKeys(db fdb.Database, n, nKeys int) error {
	start := time.Now()
	for i := 0; i < n; i++ {
		mykey := fmt.Sprintf("hello-%d", rand.Intn(nKeys))
		_, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
			ret = tr.Get(fdb.Key(mykey)).MustGet()
			return
		})
		if err != nil {
			return errors.Wrap(err, "Unable to read FDB database value")
		}
	}
	elapsed := time.Now().Sub(start)
	rate := float64(n) / elapsed.Seconds()
	log.Printf("get completed %d iterations with rate %f", n, rate)
	return nil
}

func setKeysConcurrent(db fdb.Database, n, nKeys, size, nConcurrent int) error {
	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(nConcurrent)
	completed := make([]int, nConcurrent)
	completedBytes := make([]int64, nConcurrent)
	for i := 0; i < nConcurrent; i++ {
		go func(id int) {
			defer wg.Done()
			buf := make([]byte, size)
			ct := 0
			var byteCt int64
			for i := 0; i < n; i++ {
				rand.Read(buf)
				mykey := fmt.Sprintf("hello-%d", rand.Intn(nKeys))
				_, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
					tr.Set(fdb.Key(mykey), buf)
					return
				})
				// if err != nil {
				// 	return errors.Wrap(err, "Unable to set FDB database")
				// }
				if err != nil {
					log.Printf("Error %v", err)
					break
				}
				ct++
				byteCt += int64(len(buf))
			}
			completed[id] = ct
			completedBytes[id] = byteCt
		}(i)
	}
	wg.Wait()
	var nComplete int
	var bytesComplete int64
	for i := 0; i < nConcurrent; i++ {
		nComplete += completed[i]
		bytesComplete += completedBytes[i]
	}

	elapsed := time.Now().Sub(start)
	rate := float64(nComplete) / elapsed.Seconds()
	byteRate := float64(bytesComplete) / elapsed.Seconds()
	log.Printf("concurrent set completed %d iterations with rate %f byte rate %f", n, rate, byteRate)
	return nil
}

func getKeysConcurrent(db fdb.Database, n, nKeys, nConcurrent int) error {
	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(nConcurrent)
	completed := make([]int, nConcurrent)
	completedBytes := make([]int64, nConcurrent)
	for i := 0; i < nConcurrent; i++ {
		go func(id int) {
			defer wg.Done()
			ct := 0
			var byteCt int64 = 0
			for i := 0; i < n; i++ {
				mykey := fmt.Sprintf("hello-%d", rand.Intn(nKeys))
				r, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
					ret = tr.Get(fdb.Key(mykey)).MustGet()
					return
				})
				if err != nil {
					log.Printf("Error %v", err)
					break
				}
				byteCt += int64(len(r.([]byte)))
				ct++
			}
			completed[id] = ct
			completedBytes[id] = byteCt
		}(i)
	}
	wg.Wait()
	var nComplete int
	var bytesComplete int64
	for i := 0; i < nConcurrent; i++ {
		nComplete += completed[i]
		bytesComplete += completedBytes[i]
	}

	elapsed := time.Now().Sub(start)
	rate := float64(nComplete) / elapsed.Seconds()
	byteRate := float64(bytesComplete) / elapsed.Seconds()
	log.Printf("concurrent get completed %d iterations with rate %f byte rate %f", n, rate, byteRate)
	return nil
}

func sayHello(db fdb.Database) error {
	_, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(fdb.Key("hello"), []byte("world"))
		return
	})
	if err != nil {
		return errors.Wrap(err, "Unable to set FDB database value")
	}
	ret, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		ret = tr.Get(fdb.Key("hello")).MustGet()
		return
	})
	if err != nil {
		return errors.Wrap(err, "Unable to read FDB database value")
	}

	v := ret.([]byte)
	fmt.Printf("hello, %s\n", string(v))
	return nil
}

func main() {
	fdb.MustAPIVersion(620)
	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	err := sayHello(db)
	if err != nil {
		log.Fatal(err)
	}

	var numBlocks, blocksPerTask, blockSize int
	var testRead, testWrite bool
	var concurrencyStr string

	flag.IntVar(&numBlocks, "num-blocks", 1000, "total number of blocks")
	flag.IntVar(&blocksPerTask, "blocks-per-task", 1000, "number of blocks per tas")
	flag.IntVar(&blockSize, "block-size", 1024, "block size")
	flag.StringVar(&concurrencyStr, "concurrency", "10", "concurrency")

	flag.BoolVar(&testRead, "read", true, "read test")
	flag.BoolVar(&testWrite, "write", true, "write test")

	flag.Parse()

	var concurrencies []int

	for _, cs := range strings.Split(concurrencyStr, ",") {
		ci, err := strconv.Atoi(cs)
		if err != nil {
			log.Fatal(err)
		}
		concurrencies = append(concurrencies, ci)
	}

	// err = setKeys(db, 100, 1000, blockSize)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// err = getKeys(db, 100, 1000)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	if testWrite {
		for _, concurrency := range concurrencies {
			err = setKeysConcurrent(db, blocksPerTask, numBlocks, blockSize, concurrency)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if testRead {
		for _, concurrency := range concurrencies {
			err = getKeysConcurrent(db, blocksPerTask, numBlocks, concurrency)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// Database reads and writes happen inside transactions
	// ret, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
	// 	tr.Set(fdb.Key("hello"), []byte("world"))
	// 	return tr.Get(fdb.Key("foo")).MustGet(), nil
	// 	// db.Transact automatically commits (and if necessary,
	// 	// retries) the transaction
	// })
	// if e != nil {
	// 	log.Fatalf("Unable to perform FDB transaction (%v)", e)
	// }

}

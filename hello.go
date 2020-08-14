package main

import (
	"fmt"
	"log"
	"math/rand"
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
	for i := 0; i < nConcurrent; i++ {
		go func(id int) {
			defer wg.Done()
			buf := make([]byte, size)
			ct := 0
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
			}
			completed[id] = ct
		}(i)
	}
	wg.Wait()
	nComplete := 0
	for _, n := range completed {
		nComplete += n
	}
	elapsed := time.Now().Sub(start)
	rate := float64(nComplete) / elapsed.Seconds()
	log.Printf("concurrent set completed %d iterations with rate %f", n, rate)
	return nil
}

func getKeysConcurrent(db fdb.Database, n, nKeys, nConcurrent int) error {
	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(nConcurrent)
	completed := make([]int, nConcurrent)
	for i := 0; i < nConcurrent; i++ {
		go func(id int) {
			defer wg.Done()
			ct := 0
			for i := 0; i < n; i++ {
				mykey := fmt.Sprintf("hello-%d", rand.Intn(nKeys))
				_, err := db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
					ret = tr.Get(fdb.Key(mykey)).MustGet()
					return
				})
				if err != nil {
					log.Printf("Error %v", err)
					break
				}
				ct++
			}
			completed[id] = ct
		}(i)
	}
	wg.Wait()
	nComplete := 0
	for _, n := range completed {
		nComplete += n
	}
	elapsed := time.Now().Sub(start)
	rate := float64(nComplete) / elapsed.Seconds()
	log.Printf("concurrent get completed %d iterations with rate %f", n, rate)
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

	blockSize := 1024 * 1024

	err = setKeys(db, 100, 1000, blockSize)
	if err != nil {
		log.Fatal(err)
	}

	err = getKeys(db, 100, 1000)
	if err != nil {
		log.Fatal(err)
	}

	for _, concurrency := range []int{16, 32} {
		err = setKeysConcurrent(db, 1000, 1000, blockSize, concurrency)
		if err != nil {
			log.Fatal(err)
		}
	}

	for _, concurrency := range []int{16, 32} {
		err = getKeysConcurrent(db, 1000, 1000, concurrency)
		if err != nil {
			log.Fatal(err)
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

package main

import (
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dustin/statcap/statstore"
)

var wg sync.WaitGroup

func maybefatal(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}

type entry struct {
	m  map[string]interface{}
	ts time.Time
}

func storer(w statstore.Storer, ch <-chan entry) {
	defer wg.Done()

	for e := range ch {
		w.Insert(e.m, e.ts)
	}
}

func main() {
	r, err := statstore.GetStoreReader(os.Args[1])
	maybefatal(err)
	defer r.Close()
	w, err := statstore.GetStorer(os.Args[2])
	maybefatal(err)
	defer w.Close()

	ch := make(chan entry, 100)

	wg.Add(1)
	go storer(w, ch)

	for {
		m, ts, err := r.Next()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Printf("Error reading an entry, stopping: %v", err)
			return
		}
		log.Printf("Recording entry from %v", ts)
		ch <- entry{m, ts}
	}
	close(ch)

	wg.Wait()
}

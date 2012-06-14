package main

import (
	"io"
	"log"
	"os"

	"github.com/dustin/statcap/statstore"
)

func maybefatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	r, err := statstore.GetStoreReader(os.Args[1])
	maybefatal(err)
	defer r.Close()
	w, err := statstore.GetStorer(os.Args[2])
	maybefatal(err)
	defer w.Close()

	for {
		m, ts, err := r.Next()
		if err == io.EOF {
			return
		}
		maybefatal(err)
		log.Printf("Recording entry from %v", ts)
		w.Insert(m, ts)
	}
}

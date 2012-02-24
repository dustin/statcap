package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"code.google.com/p/dsallings-couch-go"
)

var couchUrl *string = flag.String("couch", "http://localhost:5984/stats",
	"Couch destination.")
var protoFile *string = flag.String("proto", "",
	"Proto document, into which timings stats will be added")
var forceProto *bool = flag.Bool("forceProto", false,
	"If true, proto will override document-specified fields")

var wg = sync.WaitGroup{}
var proto map[string]interface{}

func recordOne(db *couch.Database, m map[string]interface{}) {
	defer wg.Done()

	// Let us first apply the proto
	for k, v := range proto {
		if _, present := m[k]; !present || *forceProto {
			m[k] = v
		}
	}

	_, _, err := db.Insert(m)
	if err != nil {
		log.Printf("Error inserting %v\n%v", m, err)
	}
}

func record(ch <-chan map[string]interface{}) {
	db, err := couch.Connect(*couchUrl)
	if err != nil {
		log.Fatalf("Error connecting to couchdb: %v", err)
	}
	for m := range ch {
		recordOne(&db, m)
	}
}

func loadProto() {
	proto = make(map[string]interface{})
	if *protoFile != "" {
		f, err := os.Open(*protoFile)
		if err != nil {
			log.Fatalf("Error opening proto file:  %v", err)
		}
		defer f.Close()
		err = json.NewDecoder(f).Decode(&proto)
		if err != nil {
			log.Fatalf("Error parsing proto: %v", err)
		}
	}
}

func main() {
	start := time.Now()
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}
	filename := flag.Arg(0)
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening input file: %v", err)
	}
	defer f.Close()

	zr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatalf("Error making gzip reader: %v", err)
	}
	d := json.NewDecoder(zr)

	loadProto()

	ch := make(chan map[string]interface{}, 10)

	for i := 0; i < 4; i++ {
		go record(ch)
	}

	written := 0
	for {
		m := map[string]interface{}{}
		err = d.Decode(&m)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Error decoding stuff: %v", err)
		}
		wg.Add(1)
		ch <- m
		written++
	}
	log.Printf("Finished reading %d records in %v",
		written, time.Now().Sub(start))

	wg.Wait()
	close(ch)
	log.Printf("Completed storage in a total of %v", time.Now().Sub(start))
}

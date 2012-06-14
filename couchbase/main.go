package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/dustin/statcap/mapconv"
	"github.com/dustin/statcap/statstore"
)

var sleepTime *uint = flag.Uint("sleep", 5,
	"Sleep time between samples")
var server *string = flag.String("server", "http://localhost:8091/",
	"couchbase cluster to connect to")
var bucket *string = flag.String("bucket", "default", "couchbase bucket name")
var outPath *string = flag.String("out", "cap.json.gz",
	"http://couch.db/path or a /file/path")
var protoFile *string = flag.String("proto", "",
	"Proto document, into which timings stats will be added")
var additionalStats *string = flag.String("stats", "timings,kvtimings",
	"stats to fetch beyond toplevel; comma separated")

// That from which we get stats
type fetcher interface {
	GetStats(which string) map[string]map[string]string
	Close()
}

func store(db statstore.Storer, ts time.Time, m map[string]interface{}) error {
	_, _, err := db.Insert(m, ts)
	if err != nil {
		log.Printf("Error inserting data:  %v", err)
	}
	return err
}

// Get stats, converting as many values to numbers as possible.
// ...unless there's no connection, in which case we'll return empty stats.
func getNumericStats(client fetcher, which string) (rv map[string]map[string]interface{}) {
	rv = make(map[string]map[string]interface{})

	if client != nil {
		tmp := client.GetStats(which)
		for k, v := range tmp {
			rv[k] = mapconv.Numerify(v, nil)
		}
	}
	return
}

func connect() *couchbase.Bucket {
	bucket, err := couchbase.GetBucket(*server, "default", *bucket)
	if err != nil {
		log.Printf("Error connecting to %s: %v", *server, err)
		return nil
	}
	return bucket
}

func fetchOnce(proto map[string]interface{}) (int, map[string]interface{}) {

	allstats := map[string]interface{}{}

	client := connect()
	if client == nil {
		log.Printf("Failed to establish connection")
		return 0, allstats
	}
	defer client.Close()

	for k, v := range proto {
		allstats[k] = v
	}
	allstats["ts"] = time.Now()
	allstats["bucket-data"] = client

	all := getNumericStats(client, "")
	captured := len(all)
	allstats["all"] = all

	if *additionalStats != "" {
		additional := strings.Split(*additionalStats, ",")

		for _, name := range additional {
			st := getNumericStats(client, name)
			captured += len(st)
			if len(st) > 0 {
				allstats[name] = st
			}
		}
	}

	return captured, allstats

}

func gatherStats(db statstore.Storer, proto map[string]interface{}) {

	running := true

	sigch := make(chan os.Signal, 10)
	signal.Notify(sigch, os.Interrupt)

	delay := time.Duration(*sleepTime) * time.Second

	for running {
		captured, allstats := fetchOnce(proto)

		if captured > 0 {
			log.Printf("Captured %d stats", captured)

			go store(db, time.Now(), allstats)
		}

		select {
		case <-time.After(delay):
			// Normal "sleep"
		case sig := <-sigch:
			running = false
			log.Printf("Got %v, shutting down.", sig)
		}
	}
}

func main() {
	flag.Parse()

	out, err := statstore.GetStorer(*outPath)
	if err != nil {
		log.Fatalf("Error creating storer: %v", err)
	}
	defer out.Close()

	proto := map[string]interface{}{}
	if *protoFile != "" {
		f, err := os.Open(*protoFile)
		if err != nil {
			log.Fatalf("Error opening proto file:  %v", err)
		}
		err = json.NewDecoder(f).Decode(&proto)
		if err != nil {
			log.Fatalf("Error parsing proto: %v", err)
		}
	}

	log.Printf("Capturing %v to %v", *server, *outPath)

	gatherStats(out, proto)
}

package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/dustin/gomemcached/client"

	"github.com/dustin/statcap/mapconv"
	"github.com/dustin/statcap/statstore"
)

var sleepTime *uint = flag.Uint("sleep", 5,
	"Sleep time between samples")
var server *string = flag.String("server", "localhost:11211",
	"memcached server to connect to")
var outPath *string = flag.String("out", "http://localhost:5984/stats",
	"http://couch.db/path or a /file/path")
var protoFile *string = flag.String("proto", "",
	"Proto document, into which timings stats will be added")
var additionalStats *string = flag.String("stats", "timings,kvtimings",
	"stats to fetch beyond toplevel; comma separated")

// That from which we get stats
type fetcher interface {
	StatsMap(which string) (map[string]string, error)
	Close()
}

func store(db statstore.Storer, m interface{}) error {
	_, _, err := db.Insert(m)
	if err != nil {
		log.Printf("Error inserting data:  %v", err)
	}
	return err
}

// Get stats, converting as many values to numbers as possible.
// ...unless there's no connection, in which case we'll return empty stats.
func getNumericStats(client fetcher, which string) (rv map[string]interface{}) {
	if client == nil {
		rv = make(map[string]interface{})
	} else {
		rv = mapconv.Numerify(client.StatsMap(which))
	}
	return
}

func connect() *memcached.Client {
	client, err := memcached.Connect("tcp", *server)
	if err != nil {
		log.Printf("Error connecting to %s: %v", *server, err)
		return nil
	}
	return client
}

func fetchOnce(client fetcher,
	proto map[string]interface{}) (fetcher, int, map[string]interface{}) {

	allstats := map[string]interface{}{}

	for k, v := range proto {
		allstats[k] = v
	}
	allstats["ts"] = time.Now().Format(time.RFC3339)

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

	return client, captured, allstats

}

func gatherStats(client fetcher, db statstore.Storer,
	proto map[string]interface{}) {

	running := true

	sigch := make(chan os.Signal, 10)
	signal.Notify(sigch, os.Interrupt)

	delay := time.Duration(*sleepTime) * time.Second

	for running {
		var captured int
		var allstats map[string]interface{}
		client, captured, allstats = fetchOnce(client, proto)

		if captured > 0 {
			log.Printf("Captured %d stats", captured)

			go store(db, &allstats)
		} else {
			if client != nil {
				client.Close()
			}
			client = connect()
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

	client := connect()
	if client == nil {
		log.Fatalf("Error making first connection to couch")
	}
	defer client.Close()

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

	gatherStats(client, out, proto)
}

package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/dsallings-couch-go"
	"github.com/dustin/gomemcached/client"
)

var sleepTime *uint = flag.Uint("sleep", 5,
	"Sleep time between samples")
var server *string = flag.String("server", "localhost:11211",
	"memcached server to connect to")
var couchUrl *string = flag.String("couch", "http://localhost:5984/stats",
	"memcached server to connect to")
var protoFile *string = flag.String("proto", "",
	"Proto document, into which timings stats will be added")
var additionalStats *string = flag.String("stats", "timings,kvtimings",
	"stats to fetch beyond toplevel; comma separated")

// That from which we get stats
type fetcher interface {
	StatsMap(which string) (map[string]string, error)
	Close()
}

type storer interface {
	Insert(m interface{}) (string, string, error)
}

func store(db storer, m interface{}) error {
	_, _, err := db.Insert(m)
	if err != nil {
		log.Printf("Error inserting data:  %v", err)
	}
	return err
}

// Convert a map with string values to a map with mixed values,
// converting strings to numbers where possible.
func numerify(in map[string]string, err error) map[string]interface{} {
	rv := map[string]interface{}{}
	if err != nil {
		log.Printf("Error getting stats: %v", err)
		return rv
	}

	for k, v := range in {
		f, err := strconv.ParseFloat(v, 64)
		if err == nil {
			rv[k] = f
		} else {
			rv[k] = v
		}
	}

	return rv
}

// Get stats, converting as many values to numbers as possible.
// ...unless there's no connection, in which case we'll return empty stats.
func getNumericStats(client fetcher, which string) (rv map[string]interface{}) {
	if client == nil {
		rv = make(map[string]interface{})
	} else {
		rv = numerify(client.StatsMap(which))
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

func gatherStats(client fetcher, db *couch.Database,
	proto map[string]interface{}) {

	for {
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

		time.Sleep(time.Duration(*sleepTime) * time.Second)
	}
}

func main() {
	flag.Parse()

	db, err := couch.Connect(*couchUrl)
	if err != nil {
		log.Fatalf("Error connecting to couch: %v", err)
	}

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

	gatherStats(client, &db, proto)
}

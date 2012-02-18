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

func store(db *couch.Database, m interface{}) {
	_, _, err := db.Insert(m)
	if err != nil {
		log.Printf("Error inserting data:  %v", err)
	}
}

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

func getNumericStats(client *memcached.Client, which string) (rv map[string]interface{}) {
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

func gatherStats(client *memcached.Client, db *couch.Database,
	proto map[string]interface{}) {

	additional := strings.Split(*additionalStats, ",")

	for {
		allstats := map[string]interface{}{}

		for k, v := range proto {
			allstats[k] = v
		}
		allstats["ts"] = time.Now().Format(time.RFC3339)

		all := getNumericStats(client, "")
		captured := len(all)
		allstats["all"] = all

		for _, name := range additional {
			st := getNumericStats(client, name)
			captured += len(st)
			allstats[name] = st
		}

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

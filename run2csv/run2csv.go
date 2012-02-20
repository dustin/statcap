package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"

	"code.google.com/p/dsallings-couch-go"
)

var couchUrl *string = flag.String("couch", "http://localhost:5984/stats",
	"Couch DB.")

type ResultRow struct {
	Key   []interface{}
	Value interface{}
}

type Results struct {
	Rows []ResultRow
}

func maybefatal(str string, err error) {
	if err != nil {
		log.Fatalf("%s: %v", str, err)
	}
}

func getResults(db couch.Database, run, which string) {
	query_results := Results{}

	err := db.Query("_design/statdata/_view/"+which,
		map[string]interface{}{
			"reduce":    false,
			"start_key": []string{run},
			"end_key":   []interface{}{run, map[string]interface{}{}},
		},
		&query_results)
	maybefatal("Error querying couchdb", err)

	if len(query_results.Rows) < 1 {
		log.Printf("WARNING:  No %v for %v", which, run)
		return
	}

	fn := run + "-" + which + ".csv"
	fout, err := os.Create(fn)
	maybefatal("Error creating csv", err)
	defer fout.Close()
	log.Printf("Writing CSV at %v", fn)
	c := csv.NewWriter(fout)

	header := []string{}
	for n := range query_results.Rows[0].Key[1:] {
		header = append(header, fmt.Sprintf("k%d", n+1))
	}
	header = append(header, "value")
	maybefatal("Error writing csv header", c.Write(header))

	for _, r := range query_results.Rows {
		out := make([]string, 0, len(header))
		for _, v := range r.Key[1:] {
			out = append(out, fmt.Sprintf("%v", v))
		}
		out = append(out, fmt.Sprintf("%v", r.Value))

		maybefatal("Error writing csv row", c.Write(out))
	}
}

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		log.Printf("Need the name of the run to grab.")
		flag.Usage()
		os.Exit(1)
	}
	run := flag.Arg(0)

	db, err := couch.Connect(*couchUrl)
	maybefatal("Error connecting to couchdb", err)

	getResults(db, run, "kvtimings")
	getResults(db, run, "timings")
	getResults(db, run, "toplevel")
}

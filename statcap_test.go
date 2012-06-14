package main

import (
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	"github.com/dustin/statcap/statstore"
)

var amap = map[string]map[string]string{
	"": map[string]string{
		"stringkey": "string",
		"intkey":    "1848",
	},
	"other": map[string]string{
		"otherkey": "otherval",
		"othernum": "8184",
	},
}

type testfetcher map[string]map[string]string

func (tf testfetcher) StatsMap(which string) (map[string]string, error) {
	return map[string]string(tf[which]), nil
}

func (tf testfetcher) Close() {
}

type teststorer struct {
	encoder *json.Encoder
}

func (ts *teststorer) Insert(it statstore.StoredItem) (string, string, error) {
	return "a", "b", ts.encoder.Encode(it)
}

func (ts *teststorer) Close() error {
	return nil
}

func TestStoring(t *testing.T) {
	storer := &teststorer{encoder: json.NewEncoder(ioutil.Discard)}

	proto := map[string]interface{}{
		"version": 1,
		"name":    "bob",
	}
	tf := testfetcher(amap)

	*additionalStats = "other,missing"

	_, _, r := fetchOnce(tf, proto)

	err := store(storer, time.Now(), r)
	if err != nil {
		t.Fatalf("Error storing value: %v", err)
	}
}

func TestGetNumericStatsNil(t *testing.T) {
	if len(getNumericStats(nil, "x")) != 0 {
		t.Fatalf("Expected empty map with nil input.")
	}
}

func TestGetNumericStats(t *testing.T) {
	tf := testfetcher(amap)

	r := getNumericStats(tf, "other")
	if len(r) != 2 {
		t.Fatalf("Expected useful results, got: %v", r)
	}

	if r["otherkey"] != "otherval" || r["othernum"] != float64(8184) {
		t.Fatalf("Got unexpected results: %v", r)
	}
}

func TestFetchOnceNoAlt(t *testing.T) {
	proto := map[string]interface{}{
		"version": 1,
		"name":    "bob",
	}
	tf := testfetcher(amap)

	*additionalStats = ""

	tfout, n, r := fetchOnce(tf, proto)
	if tfout == nil {
		t.Fatalf("Ate my fetcher: %v != %v", tfout, tf)
	}

	if n != 2 {
		t.Fatalf("Expected two total stats, got %v -- %+v", n, r)
	}

	// my proto + timestamp + the stat results
	if len(r) != 4 {
		t.Fatalf("Expected useful results, got: %v (%d)", r, len(r))
	}

	all := r["all"].(map[string]interface{})

	if all["stringkey"] != "string" || all["intkey"] != float64(1848) {
		t.Fatalf("Got unexpected results: %v", all)
	}
}

func TestFetchOnceWithOneAlt(t *testing.T) {
	proto := map[string]interface{}{
		"version": 1,
		"name":    "bob",
	}
	tf := testfetcher(amap)

	*additionalStats = "other"

	tfout, n, r := fetchOnce(tf, proto)
	if tfout == nil {
		t.Fatalf("Ate my fetcher: %v != %v", tfout, tf)
	}

	if n != 4 {
		t.Fatalf("Expected two total stats, got %v -- %+v", n, r)
	}

	// my proto + timestamp + the stat results
	if len(r) != 5 {
		t.Fatalf("Expected useful results, got: %v (%d)", r, len(r))
	}

	all := r["all"].(map[string]interface{})

	if all["stringkey"] != "string" || all["intkey"] != float64(1848) {
		t.Fatalf("Got unexpected results: %v", all)
	}

	other := r["other"].(map[string]interface{})

	if other["otherkey"] != "otherval" || other["othernum"] != float64(8184) {
		t.Fatalf("Got unexpected results: %v", other)
	}
}

func TestFetchOnceWithTwoAltOneMissing(t *testing.T) {
	proto := map[string]interface{}{
		"version": 1,
		"name":    "bob",
	}
	tf := testfetcher(amap)

	*additionalStats = "other,missing"

	tfout, n, r := fetchOnce(tf, proto)
	if tfout == nil {
		t.Fatalf("Ate my fetcher: %v != %v", tfout, tf)
	}

	if n != 4 {
		t.Fatalf("Expected two total stats, got %v -- %+v", n, r)
	}

	// my proto + timestamp + the stat results
	if len(r) != 5 {
		t.Fatalf("Expected useful results, got: %v (%d)", r, len(r))
	}

	all := r["all"].(map[string]interface{})

	if all["stringkey"] != "string" || all["intkey"] != float64(1848) {
		t.Fatalf("Got unexpected results: %v", all)
	}

	other := r["other"].(map[string]interface{})

	if other["otherkey"] != "otherval" || other["othernum"] != float64(8184) {
		t.Fatalf("Got unexpected results: %v", other)
	}
}

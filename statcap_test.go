package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"testing"
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

func TestNumerify(t *testing.T) {
	m2 := numerify(amap[""], nil)
	if _, ok := m2["stringkey"].(string); !ok {
		t.Fatalf("Expected a string for stringkey, didn't get it")
	}
	if _, ok := m2["intkey"].(float64); !ok {
		t.Fatalf("Expected a float64 for intkey, didn't get it")
	}
}

func TestNumerifyWithError(t *testing.T) {
	e := errors.New("crap")
	m2 := numerify(nil, e)
	if len(m2) != 0 {
		t.Fatalf("Expected empty map, got: %v", m2)
	}
}

type testfetcher map[string]map[string]string

func (tf testfetcher) StatsMap(which string) (map[string]string, error) {
	return map[string]string(tf[which]), nil
}

func (tf testfetcher) Close() {
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

type teststorer struct {
	encoder *json.Encoder
}

func (ts *teststorer) Insert(m interface{}) (string, string, error) {
	return "a", "b", ts.encoder.Encode(m)
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

	err := store(storer, r)
	if err != nil {
		t.Fatalf("Error storing value: %v", err)
	}
}

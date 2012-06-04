package mapconv

import (
	"errors"
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
	m2 := Numerify(amap[""], nil)
	if _, ok := m2["stringkey"].(string); !ok {
		t.Fatalf("Expected a string for stringkey, didn't get it")
	}
	if _, ok := m2["intkey"].(float64); !ok {
		t.Fatalf("Expected a float64 for intkey, didn't get it")
	}
}

func TestNumerifyWithError(t *testing.T) {
	e := errors.New("crap")
	m2 := Numerify(nil, e)
	if len(m2) != 0 {
		t.Fatalf("Expected empty map, got: %v", m2)
	}
}

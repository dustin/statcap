package statstore

import (
	"encoding/json"
	"strings"
	"time"
)

type StoredItem struct {
	ts   *time.Time
	rawJ *json.RawMessage
	rawI *interface{}
}

func (s StoredItem) MarshalJSON() (rv []byte, err error) {
	switch {
	case s.rawJ != nil:
		rv = []byte(*s.rawJ)
	case s.rawI != nil:
		rv, err = json.Marshal(s.rawI)
	default:
		panic("invalid item")
	}
	return
}

func (s *StoredItem) UnmarshalJSON(in []byte) error {
	d := make([]byte, len(in))
	copy(d, in)
	rm := json.RawMessage(d)
	s.rawJ = &rm
	return nil
}

func (s *StoredItem) extractRawTS() time.Time {
	out := struct {
		TS time.Time
	}{}
	err := json.Unmarshal([]byte(*s.rawJ), &out)
	if err != nil {
		panic("Error reading timestamp from raw JSON: " + err.Error())
	}
	s.ts = &out.TS
	return out.TS
}

func NewItem(data interface{}, ts time.Time) (rv StoredItem) {
	rv.rawI = &data
	rv.ts = &ts
	return
}

func (s *StoredItem) extractITS() time.Time {
	if m, ok := (*s.rawI).(map[string]interface{}); ok {
		switch i := m["ts"].(type) {
		case time.Time:
			s.ts = &i
			return *s.ts
		case string:
			ts, err := time.Parse(time.RFC3339Nano, i)
			if err != nil {
				panic("Error parsing time: " + err.Error())
			}
			s.ts = &ts
			return *s.ts
		}
	}
	panic("Couldn't extract timestamp from interface{}")
}

func (s *StoredItem) UnmarshalInto(i interface{}) error {
	return json.Unmarshal([]byte(*s.rawJ), i)
}

func (s *StoredItem) Timestamp() (ts time.Time) {
	switch {
	case s.ts != nil:
		ts = *s.ts
	case s.rawJ != nil:
		ts = s.extractRawTS()
	case s.rawI != nil:
		ts = s.extractITS()
	default:
		panic("No data found for extracting timestamp.")
	}
	return
}

// Interface for things that store things.
type Storer interface {
	Insert(m StoredItem) (string, string, error)
	Close() error
}

// Interface for reading stored things.
type Reader interface {
	Next() (StoredItem, error)
	Close() error
}

// Get a storer for the given path.
func GetStorer(path string) (Storer, error) {
	if strings.HasPrefix(path, "http://") {
		return openCouchStorer(path)
	}
	if strings.HasSuffix(path, ".zip") {
		return openZipStorer(path)
	}
	return openFileStorer(path)
}

// Get a storer reader for the given path.
func GetStoreReader(path string) (Reader, error) {
	if strings.HasSuffix(path, ".zip") {
		return openZipReader(path)
	}
	return openFileReader(path)
}
